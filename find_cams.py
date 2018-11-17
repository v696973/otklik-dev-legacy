#!/usr/bin/python3
import os
import json
import argparse
from datetime import datetime,timedelta

PATH = 'data/filestore/scandb/data_blocks/'
PORTS = {"37777", "37778"}

def find_data(path=PATH):
    for file in os.listdir(path):
        if file.endswith('.json'):
            yield file

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--last-hours', help='data scanned in last X hours', type=float, default=24000)
    args = parser.parse_args()

    from_time = (datetime.now() - timedelta(hours=args.last_hours)).timestamp()
    files = find_data()

    for file in files:
        filename = os.path.join(PATH, file)
        json_data = json.load(open(filename))
        if json_data['data']['timestamp'] > from_time:
            results = json_data['data']['scan_data']
            for ip, data in results.items():
                for port in PORTS:
                    if port in data['ports'].keys():
                        print("%s:%s" % (ip, port))
