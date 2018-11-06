import requests
import os
import shutil
import ujson as json
import time
import uuid
import base58
import base64
import urllib3
from urllib.parse import quote_plus
import subprocess
import signal
import queue
from threading import Thread
from config import config
from logger import get_logger


class IPFSException(Exception):
    pass


class IPFSNode(object):

    def __init__(self):
        self.session = requests.Session()
        self.logger = get_logger('IPFS', config['ipfs']['log_level'])
        self.pubsub_topic = config['ipfs']['pubsub_topic']
        self.api_path = 'http://localhost:5001/api/v0/'

        with open('/data/ipfs/config', 'r') as f:
            ipfs_config = json.load(f)

        self.ipfs_peer_id = ipfs_config['Identity']['PeerID']
        self.pending_peers = []
        self.swarm_peers = []
        self.ipfs_pubsub_peers = []
        self.bootstrap_table = {}
        self.peer_ttl = config['ipfs']['bootstrap_peer_ttl']
        self.start_time = time.time()
        self.timeout_queue = queue.Queue()

    @property
    def daemon_is_alive(self):
        try:
            self.session.get(self.api_path)
            return True
        except (urllib3.exceptions.NewConnectionError,
                requests.exceptions.ConnectionError):
            return False

    def wait_for_ipfs_daemon(self):
        while not self.daemon_is_alive:
            time.sleep(2)

        return True

    def wait_for_ipfs_pubsub(self):
        while not self.daemon_is_alive and not self.pubsub_peers():
            time.sleep(2)

        return True

    def load_local_bootstrap_table(self):
        if os.path.isfile('/data/otklik_peers.json'):
            with open('/data/otklik_peers.json', 'r') as f:
                bootstrap_peers = json.load(f)
        else:
            bootstrap_peers = config['ipfs']['bootstrap_peers']

        bootstrap_table = {}
        for addr in bootstrap_peers:
            bootstrap_table[addr] = time.time()

        return bootstrap_table

    def load_bootstrap_table(self):
        if not self.bootstrap_table:
            self.bootstrap_table = self.load_local_bootstrap_table()

        connected_peers = self.pubsub_peers(self.pubsub_topic)
        self.pending_peers = list(
            set(connected_peers).union(set(self.pending_peers))
        )

        for peer_id in self.pending_peers:
            multiaddresses = self.get_peer_addresses(peer_id)
            bootstrap_addrs = list(self.bootstrap_table.keys())
            for addr in multiaddresses:

                (_,
                 ip_proto,
                 ip_addr,
                 proto,
                 port,
                 hl_proto,
                 peer_id) = addr.split('/')
                if addr not in bootstrap_addrs and port == '4001':
                    self.bootstrap_table[addr] = time.time()

        self.pending_peers = []
        return self.bootstrap_table

    def save_bootstrap_table(self):
        bootstrap_peers = [entry[0] for entry in sorted(
            list(self.bootstrap_table.items()),
            key=lambda x: abs(time.time() - x[1]),
        )]

        with open('/data/otklik_peers.json', 'w') as f:
            json.dump(bootstrap_peers, f)

    def is_pinned(self, ipfs_hash):
        api_url = '{}pin/ls?arg={}&type=all'.format(
            self.api_path,
            ipfs_hash
        )

        pin_data = self.session.post(
            api_url,
        ).json()

        if 'Keys' in pin_data and ipfs_hash in pin_data['Keys']:
            return True

        return False

    def add(self, data, sub_path='', use_filestore=True):
        self.wait_for_ipfs_daemon()
        if use_filestore:
            # TODO: uncomment this later when IPFS team fixes their API
            # temp_fname = os.path.join(
            #     config['ipfs']['filestore_path'],
            #     '{}.json'.format(uuid.uuid4())
            # )
            # with open(temp_fname, 'w') as data_file:
            #     data = json.dump(data, data_file, sort_keys=True)

            # api_url = '{}add?nocopy=true&only-hash=true'.format(
            #     self.api_path,
            # )
            # with open(temp_fname, 'r') as data_file:
            #     ipfs_hash = self.session.post(
            #         api_url,
            #         files={
            #             '_': ('_', data_file, 'text/plain')
            #         }
            #     ).json()['Hash']
            #     self.logger.critical(ipfs_hash)

            # data_fname = os.path.join(
            #     config['ipfs']['filestore_path'],
            #     '{}.json'.format(ipfs_hash)
            # )
            # os.rename(temp_fname, data_fname)

            # api_url = '{}add?nocopy=true'.format(
            #     self.api_path,
            # )

            # with open(temp_fname, 'r') as data_file:
            #     final_ipfs_hash = self.session.post(
            #         api_url,
            #         files={
            #             '_': ('_', data_file, 'text/plain')
            #         }
            #     ).json()['Hash']

            # assert ipfs_hash == final_ipfs_hash
            # return final_ipfs_hash

            # Create a temporary file name that later will be replaced with the
            # correct IPFS hash of the file
            temp_fname = os.path.join(
                config['ipfs']['filestore_path'],
                sub_path,
                '{}.json'.format(uuid.uuid4())
            )

            # Write the file to the filestore
            if not os.path.exists(os.path.dirname(temp_fname)):
                os.makedirs(os.path.dirname(temp_fname))

            with open(temp_fname, 'w') as data_file:
                json.dump(data, data_file, sort_keys=True)

            # Use CLI to get the hash of the file
            env = os.environ.copy()
            env['IPFS_PATH'] = '/data/ipfs'
            p = subprocess.Popen(
                ['ipfs', 'add', '--nocopy', '-n', '-Q', temp_fname],
                stdout=subprocess.PIPE,
                env=env
            )

            out, err = p.communicate()
            ipfs_hash = out.decode().splitlines()[0]

            # Rename the file using the hash
            data_fname = os.path.join(
                config['ipfs']['filestore_path'],
                sub_path,
                '{}.json'.format(ipfs_hash)
            )
            shutil.move(temp_fname, data_fname)

            # Actually add the file
            env = os.environ.copy()
            env['IPFS_PATH'] = '/data/ipfs'
            p = subprocess.Popen(
                ['ipfs', 'add', '--nocopy', '-Q', data_fname],
                stdout=subprocess.PIPE,
                env=env
            )

            out, err = p.communicate()
            final_ipfs_hash = out.decode().splitlines()[0]

            assert ipfs_hash == final_ipfs_hash
            return final_ipfs_hash

    def get(self, ipfs_hash, sub_path='', pin=True, timeout=180):
        self.wait_for_ipfs_daemon()

        def watchdog(watch_queue, status_queue, pid):
            last_alive = time.time()

            while True:
                if (time.time() - last_alive) > timeout:
                    os.kill(pid, signal.SIGTERM)
                    status_queue.put(False)
                    return False
                try:
                    watch = watch_queue.get_nowait()

                    if 'done' in watch:
                        status_queue.put(True)
                        return True
                    else:
                        last_alive = time.time()

                except queue.Empty:
                    time.sleep(0.1)
                    continue

        watch_queue = queue.Queue()
        status_queue = queue.Queue()

        data_fname = os.path.join(
            config['ipfs']['filestore_path'],
            sub_path,
            '{}.json'.format(ipfs_hash)
        )

        if os.path.isfile(data_fname):
            with open(data_fname, 'r') as data_file:
                return json.load(data_file)

        temp_fname = os.path.join('/tmp', ipfs_hash)

        env = os.environ.copy()
        env['IPFS_PATH'] = '/data/ipfs'
        proc = subprocess.Popen(
            ['ipfs', 'get', ipfs_hash, '-o', temp_fname],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            bufsize=0,
            universal_newlines=True,
        )

        watch_queue.put({'seconds': timeout})
        watchdog_thread = Thread(target=watchdog, args=(watch_queue,
                                                        status_queue,
                                                        proc.pid))
        watchdog_thread.start()
        for line in proc.stdout:
            watch_queue.put({'seconds': timeout})
            if not line:
                break
        watch_queue.put({'done': True})
        watchdog_thread.join()
        status = status_queue.get()
        if not status:
            raise IPFSException

        with open(temp_fname, 'r') as temp_data_file:
            data = json.load(temp_data_file)

        env = os.environ.copy()
        env['IPFS_PATH'] = '/data/ipfs'
        proc = subprocess.Popen(
            ['ipfs', 'repo', 'gc'],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        proc.communicate()

        if pin and not self.is_pinned(ipfs_hash):
            data_fname = os.path.join(
                config['ipfs']['filestore_path'],
                sub_path,
                '{}.json'.format(ipfs_hash)
            )

            if not os.path.isfile(data_fname):
                if not os.path.exists(os.path.dirname(data_fname)):
                    os.makedirs(os.path.dirname(data_fname))
                shutil.move(temp_fname, data_fname)

            env = os.environ.copy()
            env['IPFS_PATH'] = '/data/ipfs'
            p = subprocess.Popen(
                ['ipfs', 'add', '--nocopy', '-Q', data_fname],
                stdout=subprocess.PIPE,
                env=env
            )
            out, err = p.communicate()
        else:
            os.remove(temp_fname)

        return data

    def rm(self, ipfs_hash, sub_path='', use_filestore=True):
        self.wait_for_ipfs_daemon()

        # Unpin object
        env = os.environ.copy()
        env['IPFS_PATH'] = '/data/ipfs'
        proc = subprocess.Popen(
            ['ipfs', 'pin', 'rm', ipfs_hash],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        proc.communicate()

        if use_filestore:
            # Remove data file
            data_fname = os.path.join(
                config['ipfs']['filestore_path'],
                sub_path,
                '{}.json'.format(ipfs_hash)
            )
            if os.path.isfile(data_fname):
                os.remove(data_fname)

        # Run gc to remove an object from the blockstore
        env = os.environ.copy()
        env['IPFS_PATH'] = '/data/ipfs'
        proc = subprocess.Popen(
            ['ipfs', 'repo', 'gc'],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        proc.communicate()

    def get_size(self, ipfs_hash):
        self.wait_for_ipfs_daemon()
        api_url = '{}object/stat?arg={}'.format(
            self.api_path,
            ipfs_hash
        )

        object_data = self.session.post(api_url).json()
        return object_data['CumulativeSize']

    def local_objects(self):
        ''' Get a list of locally stored IPFS objects'''

        env = os.environ.copy()
        env['IPFS_PATH'] = '/data/ipfs'
        p = subprocess.Popen(
            ['ipfs', 'refs', 'local'],
            stdout=subprocess.PIPE,
            env=env
        )

        out, err = p.communicate()
        return out.decode().splitlines()

    def swarm_connect(self, peer_multiaddress):
        api_url = '{}swarm/connect?arg={}'.format(
            self.api_path,
            peer_multiaddress
        )

        r = self.session.post(api_url)
        return r.json()

    def swarm_disconnect(self, peer_multiaddress):
        api_url = '{}swarm/disconnect?arg={}'.format(
            self.api_path,
            peer_multiaddress
        )

        r = self.session.post(api_url)
        return r.json()

    def swarm_filter_add(self, peer_multiaddress):
        api_url = '{}swarm/filters/add?arg={}'.format(
            self.api_path,
            peer_multiaddress
        )

        r = self.session.post(api_url)
        return r.json()

    def get_swarm_peers(self):
        api_url = '{}swarm/peers'.format(self.api_path)
        peers = self.session.post(api_url).json()
        return peers['Peers'] if peers['Peers'] else []

    def get_providers(self, ipfs_hash):
        env = os.environ.copy()
        env['IPFS_PATH'] = '/data/ipfs'
        p = subprocess.Popen(
            ['ipfs', 'dht', 'findprovs', str(ipfs_hash)],
            stdout=subprocess.PIPE,
            env=env
        )

        out, err = p.communicate()
        return [peer_id for peer_id in out.decode().splitlines()]

    def get_peer_addresses(self, peer_id):
        # TODO: uncomment this later when IPFS team fixes their API
        # api_url = '{}dht/findpeer?arg={}'.format(
        #     self.api_path,
        #     peer_id
        # )

        # r = self.session.post(api_url)
        # print(peer_id)
        # print(r.text)
        # data = r.json()['Responses']['Addrs']
        # return data
        env = os.environ.copy()
        env['IPFS_PATH'] = '/data/ipfs'
        p = subprocess.Popen(
            ['ipfs', 'dht', 'findpeer', str(peer_id)],
            stdout=subprocess.PIPE,
            env=env
        )

        out, err = p.communicate()
        return [addr + '/ipfs/' + peer_id
                for addr in out.decode().splitlines()]

    def subscribe(self, topic, discover=True):
        # http://localhost:5001/api/v0/pubsub/sub?arg=<topic>&discover=true
        api_url = '{}pubsub/sub?arg={}&discover={}'.format(
            self.api_path,
            topic,
            json.dumps(discover))

        r = self.session.get(api_url, stream=True)

        self.logger.info(
            "IPFS: Listening to '{}'...".format(self.pubsub_topic)
        )

        for line in r.iter_lines():
            line = line.decode()
            if line:  # filter out keep-alive new lines
                msg = json.loads(line)
                if 'data' in msg and 'from' in msg:
                    data = base64.b64decode(msg['data']).decode()
                    peer_id = base58.b58encode(base64.b64decode(msg['from']))
                    try:
                        data = json.loads(data)
                    except json.decoder.JSONDecodeError:
                        continue

                    data['peer_id'] = peer_id
                    if data['peer_id'] != self.ipfs_peer_id:
                        yield data

    def _publish(self, topic, message):
        # http://localhost:5001/api/v0/pubsub/pub?arg=<topic>&arg=<data>
        api_url = '{}pubsub/pub?arg={}&arg={}'.format(self.api_path,
                                                      topic,
                                                      quote_plus(message))
        self.session.post(api_url)

    def publish(self, data):
        data = json.dumps(data)
        self._publish(self.pubsub_topic, data)

    def pubsub_peers(self, topic):
        api_url = '{}pubsub/peers?arg={}'.format(self.api_path, topic)
        r = self.session.get(api_url)
        return r.json()['Strings']
        # return r.text.replace('"', '').splitlines() or []  # WTF???

    def find_peers(self):
        self.logger.debug("IPFS: Searching for pubsub peers...")
        peers = self.pubsub_peers(self.pubsub_topic)
        if peers:
            if set(peers) != set(self.ipfs_pubsub_peers):
                self.logger.info(
                    "IPFS: Found {} pubsub peers after {} seconds of waiting.".format(  #NOQA
                        len(peers),
                        time.time() - self.start_time
                    ))
            # else:
            #     self.logger.info(
            #         "IPFS: Connected to {} pubsub peers.".format(
            #             len(peers)
            #         ))

            self.ipfs_pubsub_peers = list(set(peers))
        else:
            self.logger.warning("IPFS: No pubsub peers found.")

    def bootstrap(self):
        while True:
            self.find_peers()
            self.load_bootstrap_table()
            self.logger.debug(
                'IPFS: Using bootstrap table with {} peer addresses.'.format(
                    len(self.bootstrap_table))
            )

            self.logger.debug('IPFS: Connecting to known Otklik peers...')
            for addr in self.bootstrap_table.copy():
                self.logger.debug(
                    'IPFS: connecting to {}...'.format(addr)
                )

                try:
                    connection = self.swarm_connect(addr)
                except:  # TODO: add proper exception
                    self.logger.debug(
                        'IPFS: Failed to connect to {}.'.format(addr)
                    )

                if 'Code' in connection and connection['Code'] == 0:
                    self.logger.debug(
                        'IPFS: Failed to connect to {}.'.format(addr)
                    )
                else:
                    self.logger.debug(
                        'IPFS: Successfully connected to {}!'.format(addr)
                    )

                    self.bootstrap_table[addr] = time.time()

                time_diff = abs(time.time() - self.bootstrap_table[addr])

                if (time_diff > self.peer_ttl
                        and len(self.bootstrap_table) > 10):
                    del self.bootstrap_table[addr]

            self.save_bootstrap_table()
            self.swarm_peers = self.get_swarm_peers()
            time.sleep(10)

    def run(self):
        while not self.daemon_is_alive:
            self.logger.warning(
                "IPFS: API gateway offline, waiting for IPFS daemon to rise..."
            )
            time.sleep(5)

        extra_connections_thread = Thread(target=self.bootstrap)
        extra_connections_thread.start()

        for data in self.subscribe(self.pubsub_topic):
            if data['peer_id'] not in self.pending_peers:
                self.pending_peers.append(data['peer_id'])
                yield data
