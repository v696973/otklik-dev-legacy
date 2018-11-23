#!/usr/bin/python3
import os
import sys
import json
from datetime import datetime, timedelta

PATH = 'data/filestore/scandb/data_blocks/'
SEARCH_PARAMS = {
    'city': None,
    'country': None,
    'port': None,
    'banner': None,
    'last_hours': 100000,
    'service': None,
    'json': False,
}

def print_help():
    print("[SHOTKLIK]: Shodan-like search interface for Otklik results database\n")
    print("Supported parameters: %s" % ", ".join(SEARCH_PARAMS.keys()))
    print("""
Examples:
./shotklik.py country:"United States" port:80
./shotklik.py city:'Moscow' port:21
./shotklik.py service:ftp json:true
./shotklik.py service:http banner:401
./shotklik.py port:37777 banner:""
./shotklik.py port:80,8000,8080 banner:router json:true
./shotklik.py port:3389 last_hours:72
""")

def find_data(path=PATH):
    for filename in os.listdir(path):
        if filename.endswith('.json'):
            yield filename

def print_result(ip, port, info):
    if not SEARCH_PARAMS['json']:
        print("%s:%s" % (ip, port))
    else:
        print({'ip': ip, 'port': port, 'info': info})

def is_in_banner(substr, banners):
    for banner in banners:
        if substr.lower() in banner['banner'].lower():
            return True
    return False

def is_have_service(service, banners):
    for banner in banners:
        if service == banner['service_name']:
            return True
    return False

if __name__ == '__main__':
    filters = sys.argv
    filters.pop(0)
    if not filters:
        print_help()
        exit(0)

    for filt in filters:
        keyval = filt.split(':')
        if len(keyval) == 2:
            if keyval[0] in SEARCH_PARAMS:
                SEARCH_PARAMS[keyval[0]] = keyval[1]
            else:
                print("Invalid filter parameter: %s" % keyval[0])
                print_help()
                exit(-1)
        else:
            print("Invalid filter: %s" % filt)
            print_help()
            exit(-1)

    from_time = (datetime.now() - timedelta(hours=float(SEARCH_PARAMS['last_hours']))).timestamp()
    files = find_data()

    ports = SEARCH_PARAMS['port'].split(',') if SEARCH_PARAMS['port'] else None

    for json_file in files:
        json_data = json.load(open(os.path.join(PATH, json_file)))
        # time filter
        if json_data['data']['timestamp'] > from_time:
            results = json_data['data']['scan_data']
            for ip, data in results.items():
                # country filter
                if data['geodata'].get('country', '') and SEARCH_PARAMS['country']:
                    if data['geodata']['country'].lower() != SEARCH_PARAMS['country'].lower():
                        continue
                # city filter
                if data['geodata'].get('city', '') and SEARCH_PARAMS['city']:
                    if data['geodata']['city'].lower() != SEARCH_PARAMS['city'].lower():
                        continue

                for port, info in data['ports'].items():
                    # ports filter
                    if ports and not port in ports:
                        continue

                    # service filter
                    if SEARCH_PARAMS['service'] and \
                      not is_have_service(SEARCH_PARAMS['service'], info['banner_info']):
                        continue

                    # show only banners with substr
                    if SEARCH_PARAMS['banner'] and \
                       is_in_banner(SEARCH_PARAMS['banner'], info['banner_info']):
                        print_result(ip, port, info)

                    # show only empty banners
                    if not SEARCH_PARAMS['banner'] and \
                      isinstance(SEARCH_PARAMS['banner'], str) and \
                       info['banner_info'] == []:
                        print_result(ip, port, info)

                    # show all banners
                    if type(SEARCH_PARAMS['banner']) == type(None):
                        print_result(ip, port, info)
