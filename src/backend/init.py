import os
import sys
import json
import shutil
import subprocess
import signal
from logger import get_logger
import time
from Crypto.PublicKey import RSA
from Crypto import Random


logger = get_logger('INIT', 'INFO')


def kill_proc_by_name(proc_name):
    p = subprocess.Popen(['ps', '-A'], stdout=subprocess.PIPE)
    out, _ = p.communicate()

    for line in out.decode().splitlines():
        if proc_name in line:
            pid = int(line.split(None, 1)[0])
            os.kill(pid, signal.SIGKILL)

    return None


def copy_dirs(source_path, destination_path, overwrite=True):
    if os.path.exists(destination_path):
        if overwrite:
            shutil.rmtree(destination_path)
        else:
            return None

    shutil.copytree(source_path, destination_path)


def move_file_if_not_exists(source_path, destination_path):
    if not os.path.isfile(destination_path):
        shutil.move(source_path, destination_path)
        return True

    return False


def execute(command_list, nowait=False, sleep=0, shell=False):
    p = subprocess.Popen(command_list, stdout=subprocess.DEVNULL, shell=shell)
    if not nowait:
        p.communicate()
    time.sleep(sleep)

# Display BlackNode Logo
with open('/assets/logo.txt', 'r') as f:
    for l in f.read().splitlines():
        print(l)


logger.info('Initializing Otklik...')
# Create config files
if not os.path.exists('/data/config/'):
    logger.info('Creating config directory...')
    os.makedirs('/data/config/')

if move_file_if_not_exists('/default_config/otklik_config.json',
                           '/data/config/otklik_config.json'):
    logger.info('Importing otklik_config.json...')

if move_file_if_not_exists('/default_config/circus.ini',
                           '/data/config/circus.ini'):
    logger.info('Importing circus.ini...')

if move_file_if_not_exists('/default_config/proxychains.conf',
                           '/data/config/proxychains.conf'):
    logger.info('Importing proxychains.conf...')

if move_file_if_not_exists('/default_config/torrc',
                           '/data/config/torrc'):
    logger.info('Importing torrc...')

for r, d, f in os.walk('/data/config'):
    os.chmod(r, 0o777)

from config import config  # NOQA

# Load torrc
shutil.copy2('/data/config/torrc', '/etc/tor/torrc')

# Load or create Tor Hidden Service config
if os.path.isfile(
    '/data/hidden_service/hostname'
) and os.path.isfile('/data/hidden_service/private_key'):
    # Hidden Service files exist, copy them and initialize Tor by running it
    copy_dirs('/data/hidden_service/', '/var/lib/tor/hidden_service/')

    execute(['/scripts/docker-entrypoint.sh', 'tor'], nowait=True, sleep=5)

else:
    # Generate new Tor Hidden Service config
    logger.info('Tor: Generating Hidden Service files...')
    os.makedirs('/var/lib/tor/hidden_service/')
    execute(['/scripts/docker-entrypoint.sh', 'tor'], nowait=True, sleep=5)

    logger.info('Tor: Copying Hidden Service files to a config directory...')
    copy_dirs('/var/lib/tor/hidden_service/', '/data/hidden_service/')

# Read Tor Hidden Service config files
logger.info('Tor: Reading Tor Hidden Service config...')
with open('/var/lib/tor/hidden_service/hostname', 'r') as f:
    hostname = f.read().strip()

logger.info('Tor: Onion hostname: {}'.format(hostname))

# OnionCat: Get IPv6 addr
logger.info('Tor: Requesting IPv6 address from OnionCat...')

c = subprocess.Popen(['ocat', '-4i', hostname], stdout=subprocess.PIPE)
ipv6, _ = c.communicate()

# ipv4 = ipv6.decode().split()[1]
ipv6 = ipv6.decode().split()[0]

# logger.info('IPv4 Address is: {}'.format(ipv4))
logger.info('Tor: IPv6 Address is: {}'.format(ipv6))

if config['ipfs']['forbid_ipv4']:
    logger.info(
        'IPFS: Setting iptables configuration...'
    )
    # Load iptables rules
    execute(['bash', '/scripts/iptables_ipfs.sh'])

else:
    logger.critical('IPFS: Skipping iptables configuration...')
    logger.critical(
        'IPFS: iptables rules were not set. This is dangerous, as it can lead to your real IP addresses being revealed to other nodes!'  #NOQA
    )
    logger.critical(
        'IPFS: It is highly recommended to forbid clearnet connections for IPFS daemon, by setting "forbid_ipv4" option to "true" in otklik_config.json file.'  #NOQA
    )

# Generate or load IPFS config
if os.path.exists('/data/ipfs/') and os.path.isfile('/data/ipfs/config'):
    # Load existing IPFS dir
    logger.info('IPFS: Found existing IPFS directory.')

    with open('/data/ipfs/config', 'r') as f:
        ipfs_config = json.load(f)

else:
    # Init IPFS config from scratch
    logger.info('IPFS: Initializing IPFS node...')
    execute(['su - ipfs-user -c "ipfs init"'], shell=True)

    logger.info('IPFS: Dumping IPFS files...')
    copy_dirs('/home/ipfs-user/.ipfs/', '/data/ipfs/')

logger.info(
    'IPFS: Adding OnionCat IPv6 address to IPFS Swarm Addresses...'
)

with open('/data/ipfs/config', 'r') as f:
    ipfs_config = json.load(f)

logger.info('IPFS: Peer ID: {}'.format(ipfs_config['Identity']['PeerID']))

ipfs_config['Addresses']['Swarm'] = [
    "/ip6/{}/tcp/4001".format(ipv6)
]

ipfs_config['Addresses']['Announce'] = [
    "/ip6/{}/tcp/4001".format(ipv6)
]

logger.info(
    'IPFS: Adding Otklik bootstrap peers...'
)

ipfs_config['Bootstrap'] = list(
    set(ipfs_config['Bootstrap']).union(
        set(config['ipfs']['bootstrap_peers'])
    )
)

# ipfs_config['Addresses']['Gateway'] = "/ip4/0.0.0.0/tcp/8080"

# Enable experimenatal filestore
ipfs_config['Experimental']['FilestoreEnabled'] = True

if not os.path.exists('/data/filestore/'):
    logger.info('Otklik: Creating IPFS Filestore directory...')
    os.makedirs('/data/filestore')

with open('/data/ipfs/config', 'w') as f:
    json.dump(ipfs_config, f)


# Change access level for IPFS config dump
execute(['chmod', '-R', '757', '/data/ipfs/'])

# Otklik User keys
if not os.path.isfile('/data/user_keys/private_key'):
    logger.info('Otklik: Generating user keys...')
    os.makedirs('/data/user_keys')
    with open('/data/user_keys/private_key', 'w') as key_file:
        random_generator = Random.new().read
        keys = RSA.generate(2048, random_generator)
        key_file.write(keys.exportKey().decode())

# Kill Tor and OnionCat so we can control them later directly via circus
kill_proc_by_name('tor')
kill_proc_by_name('ocat')


# Create MongoDB directory
if not os.path.exists('/data/mongodb/'):
    logger.info('Creating MongoDB data directory...')
    os.makedirs('/data/mongodb/')


# masscan
# Firewall masscan port in order to perform banner checking
# Load iptables rules

logger.info('masscan: Loading iptables rules...')
execute(['bash', '/scripts/iptables_masscan.sh'])

logger.info(
    'Initialization is complete. Transfering control to a circus daemon...'
)


sys.exit(1)
