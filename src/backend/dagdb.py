from collections import defaultdict
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor
import sys
import traceback
import queue
import proof_of_work
import json
import random
import time
import dag
import ipfs
from logger import get_logger


class StackTracedThreadPoolExecutor(ThreadPoolExecutor):

    def submit(self, fn, *args, **kwargs):
        """Submits the wrapped function instead of `fn`"""

        return super(StackTracedThreadPoolExecutor, self).submit(
            self._function_wrapper, fn, *args, **kwargs)

    def _function_wrapper(self, fn, *args, **kwargs):
        """Wraps `fn` in order to preserve the traceback of any kind of
        raised exception

        """
        try:
            return fn(*args, **kwargs)
        except Exception:
            raise sys.exc_info()[0](traceback.format_exc())


class DAGDB(object):

    def __init__(self,
                 ipfs_instance,
                 identity,
                 username,
                 index_path=None,
                 data_path=None,
                 write_buffer_path=None,
                 heads_file_path=None,
                 genesis_nodes=[],
                 msg_pool_executor_size=16,
                 n_download_threads=8,
                 index_node_sizes=[100, 10000],  # sizes are in bytes
                 data_block_sizes=[10000, 20000000],
                 verify_data_blocks=True,  # Check whether data blocks are valid
                 sync_data_blocks=True,  # Download data blocks automatically
                 logger_name='DAGDB',
                 log_level='INFO'):

        self.dag = dag.DAG()
        self.p_o_w = proof_of_work.Argon2PoW()
        self.identity = identity
        self.ipfs = ipfs_instance
        self.proposed_heads = {}
        self.mutex = Lock()

        self.main_executor = StackTracedThreadPoolExecutor(
            msg_pool_executor_size
        )
        self.n_download_threads = n_download_threads
        self.pubsub_callbacks = defaultdict(
            lambda: lambda x: None,
            {
                'ping': self.on_ping,
                'new_index_node': self.on_new_index_node
            }

        )
        self.username = username
        self.logger = get_logger(logger_name, log_level)
        self.index_path = index_path
        self.data_path = data_path
        self.write_buffer_path = write_buffer_path
        self.heads_file_path = heads_file_path
        self.pending_heads = queue.Queue()

        self.genesis_nodes = genesis_nodes
        self.index_node_sizes = sorted(index_node_sizes)
        self.data_block_sizes = sorted(data_block_sizes)

        self.data_blocks_queue = queue.PriorityQueue()
        self.verify_data_blocks = verify_data_blocks
        self.sync_data_blocks = sync_data_blocks
        self.NODE_UNAVAILABLE = 404  # Not found
        self.NODE_INVALID = 422  # Unprocessable entity (RFC 4918)
        self.NODE_SAVED = 202  # Accepted

    def validate_msg(self, msg):
        # message structure
        # msg = {
        #     'peer_id': peer_id,
        #     'message': {
        #         'type': type,
        #         'author': username,
        #         'data': {},
        #         'timestamp': timestamp
        #     },
        #     'pubkey': pubkey,
        #     'signature': signature
        # }
        return (True, '')

    def on_invalid_message(self, msg, status):
        pass

    def send_pubsub_msg(self, msg_type, data):
        payload = {
            'type': msg_type,
            'author': self.username,
            'data': data,
            'timestamp': time.time()
        }

        msg = {
            'peer_id': '',  # We need this field for the message to be valid
            'message': payload,
            'pubkey': self.identity.public_key,
            'signature': self.identity.sign(payload)
        }
        # Make sure our message is valid
        is_valid_msg, status = self.validate_msg(msg)
        if is_valid_msg:
            self.ipfs.publish(msg)

    def process_pubsub_msg(self, msg):
        is_valid_msg, status = self.validate_msg(msg)
        if is_valid_msg:
            self.main_executor.submit(
                self.pubsub_callbacks[msg['message']['type']],
                msg
            )
        else:
            self.on_invalid_message(msg, status)

    def validate_index_node_size(self, node_size):
        return (True, {})

    def validate_index_node(self, node_addr, node):
        return (True, {})

    def validate_data_block_size(self, node_size):
        return (True, {})

    def validate_data_block(self, block):
        return (True, {})

    def add_new_index_node(self, data_addr):
        '''Add a new node to the local DAG and broadcast in to the network'''
        # self.status = 'Adding new node'
        # Select two random head nodes from the DAG
        heads = self.dag.heads()
        secondary_heads = set()
        if len(heads) < 2:
            if len(heads) == 0:
                return False
            else:
                for head in heads:
                    secondary_heads.update(self.dag.downstream(head))
                if len(secondary_heads) and heads:
                    secondary_heads.update(heads)
                    heads = list(secondary_heads)

        selected_heads = random.sample(heads, 2)
        node_data = {
            'author': self.username,
            'data_addr': data_addr,
            'timestamp': round(time.time(), 3),
            'prev_nodes': selected_heads
        }

        node = {
            'data': node_data,
            'nonce': self.p_o_w.prove(node_data),
            'pubkey': self.identity.public_key,
            'signature': self.identity.sign(node_data)
        }

        node_addr = self.ipfs.add(node, self.index_path)
        self.pending_heads.put([node_addr])
        # self.dag.add_node(node_addr, node)
        self.broadcast_new_index_node(node_addr)

        with open(self.heads_file_path, 'w') as f:
            json.dump(self.dag.heads(), f)

        # self.status = 'New node added'
        return node_addr, node

    def on_pending_index_node(self, node_addr):
        pass

    def on_saved_index_node(self, node_addr):
        pass

    def on_invalid_index_node(self, node_addr):
        pass

    def on_unavailable_index_node(self, node_addr):
        pass

    def get_index_node(self, node_addr, n_tries=5):
        # self.logger.debug('Downloading index node {}:'.format(node_addr))
        self.on_pending_index_node(node_addr)
        # We first get the size of the node
        # in order to make sure it's within accepted range

        index_node_size = None

        for i in range(n_tries):
            try:
                # TODO: add timeout to this method
                index_node_size = self.ipfs.get_size(node_addr)
                self.logger.debug(
                    'Index node size: {}'.format(index_node_size)
                )

                break
            except ipfs.IPFSException:
                if i == n_tries - 1:
                    # Couldn't find the node, firing callback
                    self.on_unavailable_index_node(node_addr)
                    return (self.NODE_UNAVAILABLE, node_addr, None)

                self.logger.debug(
                    'Failed to get the size of index node {}: read timeout'.format(  # NOQA
                        node_addr
                    )
                )

        is_valid_node_size, status = self.validate_index_node_size(
            index_node_size
        )
        if not is_valid_node_size:
            self.on_invalid_index_node(node_addr, None, status)
            return (self.NODE_INVALID, node_addr, None)

        for i in range(n_tries):
            try:
                node_data = self.ipfs.get(node_addr, self.index_path, pin=True)
                self.logger.debug(
                    'Index node {} has been downloaded'.format(node_addr)
                )

                self.logger.debug('Starting node validation...')
                is_valid_index_node, status = self.validate_index_node(
                    node_addr,
                    node_data
                )
                self.logger.debug('Node validated')
                if is_valid_index_node:
                    self.on_saved_index_node(node_addr, node_data)
                    return (self.NODE_SAVED, node_addr, node_data)
                else:
                    self.on_invalid_index_node(node_addr, node_data, status)
                    return (self.NODE_INVALID, node_addr, None)

            except ipfs.IPFSException:
                self.logger.debug(
                    'Failed to fetch index node {}: read timeout'.format(
                        node_addr
                    )
                )

        self.on_unavailable_index_node(node_addr)
        return (self.NODE_UNAVAILABLE, node_addr, None)

    def on_pending_data_block(self, node_addr):
        pass

    def on_saved_data_block(self, node_addr, block_data):
        pass

    def on_invalid_data_block(self, node_addr, block_data, status):
        pass

    def on_unavailable_data_block(self, node_addr):
        pass

    def get_data_block(self, index_node_addr):
        data_addr = self.dag.graph[index_node_addr]['data']['data_addr']
        self.logger.debug('Checking size of data_block: {}'.format(data_addr))
        self.on_pending_data_block(index_node_addr, data_addr)
        if self.verify_data_blocks:
            data_block_size = None
            # We get the size of the data block in order to verify
            # whether its size is within accepted range

            try:
                data_block_size = self.ipfs.get_size(data_addr)
                self.logger.debug(
                    'Data block size: {}'.format(data_block_size)
                )
            except ipfs.IPFSException:
                self.on_unavailable_data_block(index_node_addr, data_addr)
                return (self.NODE_UNAVAILABLE, data_addr, None)

                self.logger.debug(
                    'Failed to get the size of data block {}: read timeout'.format(  # NOQA
                        data_addr
                    )
                )

            is_valid_block_size, status = self.validate_data_block_size(
                data_block_size
            )

            if not is_valid_block_size:
                self.on_invalid_data_block(index_node_addr,
                                           data_addr,
                                           None,
                                           status)
                return (self.NODE_INVALID, index_node_addr, None)

        self.logger.debug('Downloading data block {}'.format(data_addr))
        try:
            block_data = self.ipfs.get(data_addr, self.data_path, pin=True)
            if self.verify_data_blocks:
                self.logger.debug('Starting data block validation...')
                is_valid_data_block, status = self.validate_data_block(
                    block_data
                )
                if is_valid_data_block:
                    self.on_saved_data_block(index_node_addr,
                                             data_addr,
                                             block_data)
                    self.logger.debug(
                        'Data block {} has been downloaded'.format(
                            data_addr
                        )
                    )
                    return (self.NODE_SAVED, data_addr, None)
                else:
                    self.on_invalid_data_block(index_node_addr,
                                               data_addr,
                                               block_data,
                                               status)
                    return (self.NODE_INVALID, index_node_addr, None)

            else:
                self.on_saved_data_block(index_node_addr,
                                         data_addr,
                                         block_data)
                self.logger.debug(
                    'Data block {} has been downloaded'.format(data_addr)
                )
                return (self.NODE_SAVED, data_addr, None)

        except ipfs.IPFSException:
            self.logger.debug(
                'Failed to fetch data block {}: read timeout'.format(
                    data_addr
                )
            )

        self.on_unavailable_data_block(index_node_addr, data_addr)
        return (self.NODE_UNAVAILABLE, index_node_addr, None)

    def add_new_data_block(self, block_data):
        # data block data structure
        # data_block = {
        #     'data': {
        #         'uploader': str,
        #         'scan_data': str,
        #         'timestamp': int,
        #     },
        #     'pubkey': pubkey,
        #     'signature': signature
        # }
        data = {
            'author': self.username,
            'scan_data': block_data,
            'timestamp': round(time.time(), 3),
        }

        data_block = {
            'data': data,
            'pubkey': self.identity.public_key,
            'signature': self.identity.sign(data)
        }

        node_addr = self.ipfs.add(data_block, 'scandb/data_blocks')
        return node_addr

    def seed_genesis_nodes(self):
        self.logger.debug('Seeding genesis nodes...')

        for index, node in enumerate(self.genesis_nodes):
            node_addr = self.ipfs.add(node, self.index_path)
            self.dag.add_node(node_addr, node)

    def on_index_dag_sync_start(self):
        pass

    def on_index_dag_sync_finish(self):
        pass

    def sync_index_dag(self, heads, pin=True):
        self.logger.debug('Starting DAG sync.')
        self.on_index_dag_sync_start()
        start_dag_len = len(self.dag.graph)
        start_time = time.time()
        executor = StackTracedThreadPoolExecutor(max_workers=8)
        heads = [head for head in heads if head not in self.dag.graph or (
            head in self.dag.graph and 'placeholder' in self.dag.graph[head]
        )]
        while True:
            new_heads = set()
            valid_nodes_present = False
            for status, node_addr, node_data in executor.map(
                self.get_index_node,
                heads
            ):
                if status != self.NODE_SAVED:
                    continue

                if self.dag.node_exists(node_addr) and node_data[
                    'data'
                ]['data_addr'] in self.ipfs.local_objects():
                    continue
                else:
                    self.dag.add_node(node_addr, node_data)
                    self.schedule_data_block_download(node_addr)
                    valid_nodes_present = True

                for prev_node_addr in node_data['data']['prev_nodes']:
                    new_heads.add(prev_node_addr)

            heads = [head for head in list(new_heads)
                     if not self.dag.node_exists(head)]

            if not valid_nodes_present:
                self.logger.error(
                    'DAG sync failed, new heads are: {}.'.format(
                        self.dag.heads()
                    )
                )
                self.logger.debug(
                    'Added {} nodes to the DAG, total nodes count: {}.'.format(
                        len(self.dag.graph) - start_dag_len,
                        len(self.dag.graph)
                    )
                )

                self.logger.debug(
                    'DAG sync took {} seconds.'.format(
                        time.time() - start_time
                    )
                )
                return False

            if all(self.dag.node_exists(node_addr) for node_addr in heads):
                break

        self.logger.debug(
            'DAG sync finished, new heads are: {}.'.format(self.dag.heads())
        )
        self.logger.debug(
            'Added {} nodes to the DAG, total nodes count: {}.'.format(
                len(self.dag.graph) - start_dag_len,
                len(self.dag.graph)
            )
        )

        self.logger.debug(
            'DAG sync took {} seconds.'.format(time.time() - start_time)
        )

        with open(self.heads_file_path, 'w') as f:
            json.dump(self.dag.heads(), f)

        self.on_index_dag_sync_finish()
        return True

    def sync_index_dag_loop(self):
        self.logger.debug('Starting sync dag loop...')
        while True:
            self.sync_index_dag(self.pending_heads.get())

    def schedule_data_block_download(self, index_node_addr, add_priority=0):
        with self.mutex:
            unique_downstream_authors = set()
            for downstream_addr in self.dag.all_downstreams(index_node_addr):
                if 'placeholder' not in self.dag.graph[downstream_addr]:
                    unique_downstream_authors.add(
                        self.dag.graph[downstream_addr]['pubkey']
                    )
        self.data_blocks_queue.put((
            -len(list(unique_downstream_authors)) + add_priority,
            index_node_addr,
        ))

    def data_block_download_worker(self):
        while True:
            if self.data_blocks_queue.empty():
                self.status['data_blocks']['sync'] = True

            node_addr = self.data_blocks_queue.get()[1]
            try:
                data_addr = self.dag.graph[node_addr]['data']['data_addr']
            except KeyError:
                continue

            if data_addr not in self.ipfs.local_objects():
                self.get_data_block(node_addr)

    def start_data_blocks_sync(self):
        self.logger.debug('Starting data blocks synchronization...')
        for i in range(self.n_download_threads):
            t = Thread(target=self.data_block_download_worker)
            t.start()

    def broadcast_new_index_node(self, node_addr):
        self.logger.debug('Broadcasting new index node.')
        self.send_pubsub_msg('new_index_node', {'index_addr': node_addr})

    def on_new_index_node(self, msg):
        if msg['message']['data']['index_addr'] not in self.dag.graph:
            self.pending_heads.put([msg['message']['data']['index_addr']])

    def on_ping(self, msg):
        heads = [h for h in msg['message']['data']['heads']
                 if h not in self.dag.graph]
        if heads:
            self.pending_heads.put(heads)

    def send_pings(self):
        while True:
            self.send_pubsub_msg('ping', {'heads': self.dag.heads()})
            time.sleep(60)

    def add(self, data):
        pass

    def generate_nodes(self):
        pass

    def run(self):
        self.ipfs.wait_for_ipfs_daemon()
        self.seed_genesis_nodes()

        try:
            with open(self.heads_file_path, 'r') as f:
                data = json.load(f)
                self.sync_index_dag(data, pin=True)
        except FileNotFoundError:
            pass

        if self.sync_data_blocks:
            self.start_data_blocks_sync()
        Thread(target=self.sync_index_dag_loop).start()
        Thread(target=self.send_pings).start()
        Thread(target=self.generate_nodes).start()

        for msg in self.ipfs.run():
            self.process_pubsub_msg(msg)


if __name__ == '__main__':
    from config import config
    from ipfs import IPFSNode
    from identity import Identity
    db = DAGDB(
        IPFSNode(),
        Identity(),
        config['main']['username'],
        index_path='scandb/index_dag',
        data_path='scandb/data_blocks',
        write_buffer_path='/data/filestore/scandb/buffer.json',
        heads_file_path='/data/filestore/scandb/dag_heads.json',
        genesis_nodes=config['scandb']['genesis_nodes'],
        log_level='DEBUG'
    )
    db.run()
