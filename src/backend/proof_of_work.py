import argon2
import hashlib
import json
import time


class PoW(object):

    def __init__(self):
        self.difficulty = None

    def preprocess_input(self, data):
        if type(data) == dict:
            data = json.dumps(data, sort_keys=True)
        else:
            data = str(data)

        return data

    def set_difficulty(self, difficulty):
        self.difficulty = difficulty

    def prove(self, data):
        data = self.preprocess_input(data)
        proof = 0
        while True:
            valid = self.validate_proof(data, proof)
            if valid:
                return proof
            else:
                proof += 1

    def validate_proof(self, data, proof):
        raise NotImplementedError


class Argon2PoW(PoW):

    def __init__(self):
        super(Argon2PoW).__init__()
        self.max_target = 2 ** 208 * 65535
        self.difficulty = 0.0000153666
        self.time_cost = 2
        self.memory_cost = 512
        self.parallelism = 1
        self.argon2_type = argon2.low_level.Type.D
        self.argon2_hash_len = 256

    def validate_proof(self, data, proof):
        target = self.max_target / self.difficulty
        guess = '{}{}'.format(proof, data).encode()
        guess_hash = hashlib.sha256(argon2.low_level.hash_secret_raw(
            guess,
            b'++++++++',
            time_cost=self.time_cost,
            memory_cost=self.memory_cost,
            parallelism=self.parallelism,
            hash_len=self.argon2_hash_len,
            type=self.argon2_type
        )).hexdigest()

        if int(guess_hash, 16) <= target:
            return guess_hash
        else:
            return False


if __name__ == '__main__':
    pow = Argon2PoW()
    start_time = time.time()
    nonce = pow.prove('Hello Otklik!')
    print('Nonce: ', nonce)
    print('Hashrate: ', nonce / (time.time() - start_time))
    print('Time spent: ', time.time() - start_time)
