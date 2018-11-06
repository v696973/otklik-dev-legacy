import subprocess
import json
import re
from geolite2 import geolite2
import random
import ipaddress
from config import config
from logger import get_logger
import time


logger = get_logger('PORT_SCANNER', config['port_scanner']['log_level'])


class PortScannerException(Exception):
    pass


class Masscan(object):

    def __init__(self,
                 masscan_path='/bin/masscan',
                 opts="-sS -Pn -n --wait 0 --banners --source-port 60000",
                 masscan_max_rate=4000):
        self.masscan_path = masscan_path
        self.opts_list = opts.split()
        self.masscan_max_rate = masscan_max_rate
        self.status = {
            'mode': 'scan',
            'bandwidth': '0.00-kpps',
            'percents_completed': '0%',
            'time_remaining': 'N/A',
            'n_processed': 0,
            'complete': False
        }

    def scan(self, ips, ports='1-65535'):
        if type(ports) == list:
            ports = ','.join([str(p) for p in ports])
        elif type(ports) == str:
            if not (re.search('^\d+-\d+$', ports) or ports.isdigit()):
                raise PortScannerException('Invalid port formatting')

        ips = ','.join([str(ip) for ip in ips])
        process_list = []
        process_list.append(self.masscan_path)
        process_list.extend(self.opts_list)
        process_list.extend(['--max-rate', str(self.masscan_max_rate)])
        process_list.extend(['-oD', '-', '-p'])
        process_list.append(ports)
        process_list.append(ips)

        result = []
        with subprocess.Popen(process_list,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT,
                              bufsize=1,
                              universal_newlines=True) as proc:
            for out in proc.stdout:
                if out and type(out) == str:
                    status_re = re.search('rate:\s+(\d+\.\d+-kpps),\s+(\d+.\d+%) done,\s+(\d+:\d+:\d+) remaining,\s+found=(\d+)', out)  # NOQA
                    if status_re:
                        self.status = {
                            'mode': 'scan',
                            'bandwidth': status_re.group(1),
                            'percents_completed': round(float(
                                re.findall('\d+\.\d+', status_re.group(2))[0],
                                ), 2),
                            'time_remaining': status_re.group(3),
                            'n_processed': int(status_re.group(4)),
                            'complete': False
                        }
                        logger.debug(
                            'Scan progress: {}% complete, {} entities found, bandwidth: {}, time remaining: {}'.format(  # NOQA
                                self.status['percents_completed'],
                                self.status['n_processed'],
                                self.status['bandwidth'],
                                self.status['time_remaining']
                            )
                        )
                    else:
                        try:
                            result.append(json.loads(out))
                        except json.decoder.JSONDecodeError:
                            continue

            logger.debug('Collecting scan output')

            self.status = {
                'mode': 'scan',
                'bandwidth': '0.00-kpps',
                'percents_completed': 100.0,
                'time_remaining': '0:00:00',
                'n_processed': int(self.status['n_processed']),
                'complete': False
            }
            return result


class PortScanner(object):

    def __init__(self):
        self.config = config
        self.geoip_reader = geolite2.reader()
        self.masscan = Masscan(
            masscan_max_rate=self.config['port_scanner']['max_rate']
        )

        self.blacklisted_subnets = [
            ipaddress.ip_network(subnet)
            for subnet in self.config['port_scanner']['blacklisted_subnets']
        ]

        self.mode = 'scan'
        self.analysis_status = {
            'mode': 'analysis',
            'percents_completed': 0.00,
            'time_remaining': 'N/A',
            'n_processed': 0,
            'complete': False
        }

    @property
    def status(self):
        if self.mode == 'scan':
            return self.masscan.status
        elif self.mode == 'analysis':
            return self.analysis_status

    def is_blacklisted_ip(self, ip):
        for subnet in self.blacklisted_subnets:
            if ip in subnet:
                return True
        return False

    def random_ip(self):
        while True:
            ip_int = random.randint(0, 4294967295)
            ip = ipaddress.ip_address(ip_int)
            if not self.is_blacklisted_ip(ipaddress.ip_address(ip_int)):
                return str(ip)

    def get_geodata(self, ip):
        try:
            geodata_raw = self.geoip_reader.get(ip)
            if geodata_raw:
                geodata = {
                    'country': (geodata_raw['country']['names']['en']
                                if 'country' in geodata_raw else None),
                    'city': (geodata_raw['city']['names']['en']
                             if 'city' in geodata_raw else None),
                    'loc': (geodata_raw['location']
                            if 'location' in geodata_raw else {})
                }
                return geodata
            else:
                return {}

        except ValueError:
            return {}

    def process_masscan_output(self, out):

        self.analysis_status = {
            'mode': 'analysis',
            'percents_completed': 0.00,
            'time_remaining': 'N/A',
            'n_processed': 0,
            'complete': False
        }

        result = {}
        for entry in out:
            ip = entry['ip']
            port = entry['port']
            if ip not in result:
                result[ip] = {
                    'ports': {},
                    'geodata': self.get_geodata(ip)
                }
            if port not in result[ip]['ports']:
                result[ip]['ports'][port] = {
                    'banner_info': [],
                }

            if entry['rec_type'] == 'banner':
                result[ip]['ports'][port]['banner_info'].append({
                    'service_name': entry['data']['service_name'],
                    'banner': entry['data']['banner']
                })
            self.analysis_status['n_processed'] += 1
            self.analysis_status['percents_completed'] = round(
                self.analysis_status['n_processed'] * 100 / len(out),
                2
            )
        self.analysis_status['complete'] = True
        return result

    def scan(self, ips, ports='1-65535'):
        logger.debug(
            'Scanning {} IP addresses, ports: {}'.format(len(ips), ports)
        )
        self.mode = 'scan'
        masscan_output = self.masscan.scan(ips, ports)
        self.mode = 'analysis'
        processed_output = self.process_masscan_output(masscan_output)
        return processed_output

    def randomscan(self):
        logger.debug('Starting random scan')
        ips = [
            self.random_ip()
            for i in range(
                self.config['port_scanner']['num_ips_per_randomscan']
            )
        ]
        scan_data = self.scan(ips)
        logger.debug('Scan has been completed')
        return scan_data


if __name__ == '__main__':
    scanner = PortScanner()
    out = scanner.scan(['45.33.32.156'])  # scanme nmap
    print(json.dumps(out, sort_keys=True, indent=4))
    time.sleep(1000)
