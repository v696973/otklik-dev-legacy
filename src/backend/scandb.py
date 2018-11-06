import ujson as json
import time
import dagdb
from threading import Thread
from pymongo import MongoClient


class MetaDB(object):

    def __init__(self):
        self.mongo = MongoClient(host='127.0.0.1')
        self.db = self.mongo['scandb']
        self.bad_peer_ids = self.db['bad_peer_ids']
        self.bad_pubkeys = self.db['bad_pubkeys']
        self.index_nodes = self.db['index_nodes']

    def add_index_node(self, node):
        assert set(node.keys()) == set(['node_addr'])

        self.bad_pubkeys.update_one(
            node,
            {'$set': node},
            upsert=True
        )

    def has_index_node(self, node_addr):
        node_data = self.bad_pubkeys.find_one(
            {'node_addr': node_addr},
        )
        if node_data:
            return True
        return False

    def remove_index_node(self, node_addr):
        self.index_nodes.delete_one({'node_addr': node_addr})

    def update_bad_network_node_rating(self, node):
        if node['pubkey']:
            self.bad_pubkeys.update_one(
                {'pubkey': node['pubkey'],
                 'ts': time.time()},
                {'$set': {'bad_rating': node['bad_rating']}},
                upsert=True
            )

        if node['peer_id']:
            self.bad_peer_ids.update_one(
                {'peer_id': node['peer_id'],
                 'ts': time.time()},
                {'$set': {'bad_rating': node['bad_rating']}},
                upsert=True
            )

    def get_bad_network_node_rating(self, node):
        if node['pubkey']:
            node_data = self.bad_pubkeys.find_one(
                {'pubkey': node['pubkey']},
            )
            if node_data:
                return node_data['bad_rating']

        if node['peer_id']:
            node_data = self.bad_peer_ids.find_one(
                {'peer_id': node['peer_id']},
            )
            if node_data:
                return node_data['bad_rating']

        return 0


class ScanDB(dagdb.DAGDB):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_block_min_count = 1  # TODO: adjust these parameters
        self.data_block_max_count = 1024  # TODO: and move it to config section
        self.status = {
            'index_dag': {
                'sync': True
            },
            'data_blocks': {
                'sync': True,
                'total': 0,
                'pending': 0
            }
        }
        self.meta_db = MetaDB()
        # self.write_buffer = self.db['write_buffer']

    # def blacklist_bad_peer_ids(self):
    #     while True:
    #         swarm_peers = self.ipfs.get_swarm_peers()
    #         if swarm_peers:
    #             self.logger.warning('Blacklisting peers')
    #             for peer_data in self.meta_db.bad_peer_ids.find():
    #                 if peer_data['bad_rating'] == 5:
    #                     multiaddresses = self.ipfs.get_peer_addresses(
    #                         peer_data['peer_id']
    #                     )
    #                     for addr in multiaddresses:
    #                         self.logger.warning('Disconnecting from peer')
    #                         self.ipfs.swarm_filter_add(addr)
    #                         self.logger.warning('Blacklisting peer')
    #                         self.ipfs.swarm_disconnect(addr)
    #             break
    #         else:
    #             time.sleep(30)

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
        status = {
            'schema_valid': False,
            'signature_valid': False,
            'author_valid': False,
            'type_valid': False
        }

        if type(msg) is not dict:
            return (False, status)

        if set(msg.keys()) != set(['peer_id',
                                   'pubkey',
                                   'signature',
                                   'message']):
            return (False, status)

        if type(msg['message']) != dict:
            return (False, status)

        if set(msg['message'].keys()) != set(['type',
                                              'author',
                                              'data',
                                              'timestamp']):
            return (False, status)

        status['schema_valid'] = True

        # Msg must have a valid signature in order to be accepted
        if not self.identity.verify_signature(msg['pubkey'],
                                              msg['signature'],
                                              msg['message']):
            return (False, status)

        status['signature_valid'] = True

        node_rating = self.meta_db.get_bad_network_node_rating({
            'pubkey': msg['pubkey'],
            'peer_id': msg['peer_id']
        })

        if node_rating == 5:
            return (False, status)

        status['author_valid'] = True

        if msg['message']['type'] not in ['ping', 'new_index_node']:
            return (False, 'msg_type_unknown')

        status['type_valid'] = True

        return (True, status)

    def validate_index_node_size(self, node_size):
        if (node_size > self.index_node_sizes[0] and
                node_size < self.index_node_sizes[1]):
            return True, {}
        else:
            return False, {'node_size_valid': False}

    def validate_data_block_size(self, block_size):
        if (block_size > self.data_block_sizes[0] and
                block_size < self.data_block_sizes[1]):
            return True, {}
        else:
            return False, {'node_size_valid': False}

    def validate_data_block(self, block):
        # TODO: Consider adding validation here?
        return True, {}

    def validate_index_node(self, node_addr, node):
        # node data structure
        # node = {
        #     'data': {
        #         'uploader': str,
        #         'data_addr': str,
        #         'timestamp': int,
        #     },
        #     'nonce': int
        #     'pubkey': pubkey,
        #     'signature': signature
        # }

        self.logger.debug('Validating index node...')

        status = {
            'schema_valid': False,
            'signature_valid': False,
            'author_valid': False,
            'pow_valid': False
        }

        if type(node) is not dict:
            return (False, status)

        # We expect the node to have 'nonce', 'signature' and 'data' keys
        # and nothing else
        if set(node.keys()) != set(['nonce', 'data', 'signature', 'pubkey']):
            return (False, status)

        self.logger.debug('node fields validation passed')
        # Nonce must be an int and data must be a dictionary
        if type(node['nonce']) != int or type(node['data']) != dict:
            return (False, status)

        self.logger.debug('nonce and data types are valid')
        # We expect node data to have 'timestamp', 'uploader'
        # and 'data_addr' fields
        if set(node['data'].keys()) != set(['timestamp',
                                            'author',
                                            'data_addr',
                                            'prev_nodes']):
            return (False, status)

        self.logger.debug('node data fields are valid')

        status['schema_valid'] = True

        # Node must have a valid signature in order to be accepted
        if not self.identity.verify_signature(node['pubkey'],
                                              node['signature'],
                                              node['data']):

            return (False, status)

        status['signature_valid'] = True

        self.logger.debug('signature is valid')

        node_rating = self.meta_db.get_bad_network_node_rating({
            'pubkey': node['pubkey'],
            'peer_id': None
        })

        if not self.meta_db.has_index_node(node_addr):
            if node_rating == 5:
                return (False, status)

        status['author_valid'] = True

        # Proof of Work nonce must be valid in order for the node
        # to be accepted
        if not self.p_o_w.validate_proof(
            self.p_o_w.preprocess_input(node['data']),
            node['nonce']
        ):
            return (False, status)

        self.logger.debug('PoW is valid')

        status['pow_valid'] = True

        self.logger.debug('Node is valid.')
        return (True, status)

    def on_invalid_message(self, msg, status):
        # Try to validate message signature:
        if status['author_valid']:
            bad_network_node = {
                'peer_id': msg['peer_id'],
                'pubkey': None,
                'bad_rating': 5
            }

            if status['signature_valid']:
                bad_network_node['pubkey'] = msg['pubkey']

            self.meta_db.update_bad_network_node_rating(bad_network_node)

    def on_index_dag_sync_start(self):
        self.status['index_dag']['sync'] = False

    def on_index_dag_sync_finish(self):
        self.status['index_dag']['sync'] = True

    def on_pending_index_node(self, node_addr):
        pass

    def on_saved_index_node(self, node_addr, node_data):
        self.status['data_blocks']['total'] = (
            len(self.dag.graph) - len(self.genesis_nodes)
        )
        self.meta_db.add_index_node({'node_addr': node_addr})

    def remove_upstream_nodes(self,
                              node_addr,
                              blacklist=True,
                              remove_index_nodes=True,
                              remove_data_blocks=True):
        upstream_addrs = self.dag.all_upstreams(node_addr)
        for node_addr in upstream_addrs:

            if blacklist and 'placeholder' not in self.dag.graph[node_addr]:
                # We assume that the nodes in the dag are valid
                # So we just blacklist them by public keys
                bad_network_node = {
                    'peer_id': None,
                    'pubkey': self.dag.graph[node_addr]['pubkey'],
                    'bad_rating': 5
                }

                self.meta_db.update_bad_network_node_rating(
                    bad_network_node
                )

            if remove_index_nodes:
                self.ipfs.rm(node_addr, self.index_path)
                self.meta_db.remove_index_node(node_addr)
                try:
                    self.dag.remove_node(node_addr)
                except KeyError:
                    pass

            if remove_data_blocks:
                try:
                    data_addr = self.dag.graph[node_addr]['data']['data_addr']
                    self.ipfs.rm(data_addr, self.data_path)
                except KeyError:
                    pass

        self.status['data_blocks']['total'] = (
            len(self.dag.graph) - len(self.genesis_nodes)
        )

    def on_invalid_index_node(self, node_addr, node_data, status):
        node_providers = self.ipfs.get_providers(node_addr)
        for peer_id in node_providers:
            bad_network_node = {
                'peer_id': peer_id,
                'pubkey': None,
                'bad_rating': 5
            }

            self.meta_db.update_bad_network_node_rating(bad_network_node)

        if node_addr in self.dag.graph:
            with self.mutex:
                self.remove_upstream_nodes(node_addr)

        self.status['data_blocks']['total'] = (
            len(self.dag.graph) - len(self.genesis_nodes)
        )

    def on_unavailable_index_node(self, node_addr):
        self.status['data_blocks']['total'] = (
            len(self.dag.graph) - len(self.genesis_nodes)
        )
        self.logger.error('Index node {} is not available'.format(node_addr))

    def on_pending_data_block(self, node_addr, data_addr):
        if (self.status['data_blocks']['pending'] <
                (len(self.dag.graph) - len(self.genesis_nodes))):
            self.status['data_blocks']['pending'] += 1

    def on_saved_data_block(self, node_addr, data_addr, block_data):
        if node_addr not in self.dag.graph:
            self.ipfs.rm(data_addr, self.data_path)
            self.logger.warning(
                'Deleted orphaned data block {}'.format(data_addr)
            )
        else:
            if self.status['data_blocks']['pending'] > 0:
                self.status['data_blocks']['pending'] -= 1
                self.status['data_blocks']['sync'] = False
            else:
                self.status['data_blocks']['sync'] = True

    def on_invalid_data_block(self, node_addr, data_addr, block_data, status):
        if node_addr in self.dag.graph:
            self.logger.error('on_invalid_data_block index validation')
            _, status = self.validate_index_node(
                node_addr,
                self.dag.graph[node_addr]
            )
            self.on_invalid_index_node(node_addr,
                                       self.dag.graph[node_addr],
                                       status)
        else:
            self.ipfs.rm(data_addr, self.data_path)
            self.logger.warning(
                'Deleted orphaned data block {}'.format(data_addr)
            )

        if self.status['data_blocks']['pending'] > 0:
            self.status['data_blocks']['pending'] -= 1
        self.status['data_blocks']['total'] -= 1

    def on_unavailable_data_block(self, node_addr, data_addr):
        self.logger.error('Data block {} is not available'.format(node_addr))
        self.schedule_data_block_download(node_addr,
                                          add_priority=len(self.dag.graph))

    def add(self, data):
        ''' Add scan results to the database '''
        self.logger.debug('Saving buffer data')
        assert type(data) == dict
        try:
            with open(self.write_buffer_path, 'r') as f:
                try:
                    self.logger.debug('Reading buffer file')
                    write_buffer = json.load(f)
                except json.decoder.JSONDecodeError:
                    write_buffer = {}
        except FileNotFoundError:
            write_buffer = {}

        self.logger.debug('Merging buffer data')
        write_buffer = {**write_buffer, **data}
        with open(self.write_buffer_path, 'w') as f:
            self.logger.debug('Writing buffer data')
            json.dump(write_buffer, f)

    def generate_nodes(self):
        while True:
            # self.logger.debug('Checking buffer...')
            try:
                with open(self.write_buffer_path) as buffer_file:
                    try:
                        write_buffer = json.load(buffer_file)
                    except json.decoder.JSONDecodeError:
                        write_buffer = {}
            except FileNotFoundError:
                write_buffer = {}

            # self.logger.debug('Buffer size: {}'.format(len(write_buffer)))
            if len(write_buffer) < self.data_block_min_count:
                time.sleep(30)
                continue

            new_write_buffer = {}
            pending_ips = list(write_buffer.keys())
            for i in range(0, len(pending_ips),
                           self.data_block_max_count):
                chunk_keys = pending_ips[
                    i: i + self.data_block_max_count
                ]
                chunk = {key: write_buffer[key] for key in chunk_keys}

                if len(chunk) < self.data_block_min_count:
                    new_write_buffer = {**new_write_buffer, **chunk}
                else:
                    self.logger.debug('Generating new node...')
                    data_block_addr = self.add_new_data_block(chunk)
                    self.logger.debug(
                        'Data block address: {}'.format(data_block_addr)
                    )
                    node_addr, node = self.add_new_index_node(data_block_addr)
                    self.logger.debug(
                        'Index node address: {}'.format(node_addr)
                    )
                    time.sleep(2)

            write_buffer = new_write_buffer
            # self.logger.debug('Buffer size: {}'.format(len(write_buffer)))

            with open(self.write_buffer_path, 'w') as f:
                json.dump(write_buffer, f)

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
        # Thread(target=self.blacklist_bad_peer_ids).start()
        Thread(target=self.sync_index_dag_loop).start()
        Thread(target=self.send_pings).start()
        Thread(target=self.generate_nodes).start()

        for msg in self.ipfs.run():
            self.process_pubsub_msg(msg)
