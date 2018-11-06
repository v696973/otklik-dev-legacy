from Crypto.PublicKey import RSA
from Crypto.Hash import SHA256
from Crypto.Signature import PKCS1_v1_5
import base64
import json


class Identity(object):

    def __init__(self):
        with open('/data/user_keys/private_key', 'r') as f:
            self.keys = RSA.importKey(f.read().encode())

    def sign(self, data):
        assert self.keys.can_sign
        if type(data) == dict:
            data = json.dumps(data, sort_keys=True)

        signer = PKCS1_v1_5.new(self.keys)
        digest = SHA256.new()
        digest.update(data.encode())
        signature = signer.sign(digest)
        return base64.b64encode(signature).decode()

    @staticmethod
    def verify_signature(public_key, signature, data):
        assert type(public_key) == str
        assert type(signature) == str
        if type(data) == dict:
            data = json.dumps(data, sort_keys=True)
        rsakey = RSA.importKey(base64.b64decode(public_key))
        signer = PKCS1_v1_5.new(rsakey)
        digest = SHA256.new()
        digest.update(data.encode())

        if signer.verify(digest, base64.b64decode(signature.encode())):
            return True

        return False

    @property
    def public_key(self):
        return base64.b64encode(
            self.keys.publickey().exportKey('DER')
        ).decode()


if __name__ == '__main__':
    id = Identity()
    signature = id.sign('Hello Otklik!')
    print(id.verify_signature(id.public_key, signature, 'Hello Otklik!'))
