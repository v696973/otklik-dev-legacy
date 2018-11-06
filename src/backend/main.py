import asyncio
import time
import json
import websockets
from threading import Thread

from config import config
from ipfs import IPFSNode
from identity import Identity
from scandb import ScanDB
from scanner import PortScanner
from logger import get_logger
import queue


class OtklikNode(object):

    def __init__(self):
        self.config = config
        self.identity = Identity()
        self.ipfs = IPFSNode()
        self.scandb = ScanDB(
            self.ipfs,
            self.identity,
            self.config['main']['username'],
            index_path='scandb/index_dag',
            data_path='scandb/data_blocks',
            write_buffer_path='/data/filestore/scandb/buffer.json',
            heads_file_path='/data/filestore/scandb/dag_heads.json',
            genesis_nodes=self.config['scandb']['genesis_nodes'],
            logger_name='SCANDB',
            log_level=self.config['scandb']['log_level'],
        )

        self.status = {}
        self.scanner = PortScanner()
        self.scan_queue = queue.Queue()

        self.logger = get_logger('MAIN', self.config['main']['log_level'])

        self.consumer_queue = queue.Queue()
        self.producer_queue = queue.Queue()

        self.status = {
            'scandb': {
                'index_dag': {
                    'sync': True
                },
                'data_blocks': {
                    'sync': True,
                    'total': 0,
                    'pending': 0
                }
            },
            'port_scanner': {
                'mode': 'scan',  # banner_grab or analysis
                'data': {}

            },
            'ipfs': {
                'pubsub_peers': 0,
                'swarm_peers': 0
            }
        }

    async def consumer_handler(self, websocket):
        while True:
            message = await websocket.recv()
            self.consumer_queue.put_nowait(json.loads(message))

    async def producer_handler(self, websocket):
        while True:
            if not self.producer_queue.empty():
                message = self.producer_queue.get_nowait()
                await websocket.send(json.dumps(message))
            else:
                await asyncio.sleep(0.5)

    async def handler(self, websocket, path):
        consumer_task = asyncio.ensure_future(self.consumer_handler(websocket))
        producer_task = asyncio.ensure_future(self.producer_handler(websocket))
        done, pending = await asyncio.wait(
            [producer_task, consumer_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

    def consumer_thread(self):
        while True:
            msg = self.consumer_queue.get()
            if msg and type(msg) is dict and 'type' in msg:
                if msg['type'] == 'config_get':
                    self.producer_queue.put({'type': 'config',
                                             'data': self.config})
                elif msg['type'] == 'config_update':
                    self.config = msg['data']
                    self.producer_queue.put({'type': 'config',
                                             'data': self.config})

                elif msg['type'] == 'db_search_query_semantic':
                    with open('result_ssh.json') as f:
                        result = json.load(f)

                    self.producer_queue.put({
                        "type": "db_search_result",
                        "data": {
                            "result": result
                        }
                    })

                else:
                    self.producer_queue.put({
                        'type': 'error',
                        'data': {'message': 'Unknown request type'}
                    })

    def producer_thread(self):
        while True:
            self.status['ipfs'] = {
                'pubsub_peers': len(self.ipfs.ipfs_pubsub_peers),
                'swarm_peers': len(self.ipfs.swarm_peers)
            }
            self.status['port_scanner'] = self.scanner.status,
            self.status['scandb'] = self.scandb.status

            self.producer_queue.put({'type': 'status', 'data': self.status})
            time.sleep(2)

    def scanner_manager(self):
        while True:
            if self.scan_queue.empty():
                self.logger.debug('Starting randomscan session')
                scan_data = self.scanner.randomscan()
                if scan_data:
                    self.logger.debug('Randomscan has been completed')
                    self.logger.debug('Adding scan results to the database')
                    self.scandb.add(scan_data)
                else:
                    self.logger.debug(
                        'Scan results are empty - nothing to add'
                    )
            else:
                scan_targets = self.scan_queue.get()
                scan_data = self.scanner.scan(
                    scan_targets['ips'],
                    ports=scan_targets['ports']
                )
                if scan_data:
                    self.scandb.add(scan_data)

    def run(self):
        scandb_thread = Thread(target=self.scandb.run)
        scandb_thread.start()

        if self.config['port_scanner']['enabled']:
            scanner_thread = Thread(target=self.scanner_manager)
            scanner_thread.start()

        if self.config['main']['serve_gui']:

            consumer_thread = Thread(target=self.consumer_thread)
            consumer_thread.start()

            status_thread = Thread(target=self.producer_thread)
            status_thread.start()
            start_server = websockets.serve(self.handler,
                                            '0.0.0.0',
                                            5678)

            asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    otklik_node = OtklikNode()
    otklik_node.run()
