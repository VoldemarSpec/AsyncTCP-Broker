import json


class Protocol:
    @staticmethod
    def encode(data):
        return json.dumps(data).encode() + b"\n"

    @staticmethod
    def decode(data):
        return json.loads(data.decode())
