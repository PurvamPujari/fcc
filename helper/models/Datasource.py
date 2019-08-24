
import json


class Datasource:
    """Datasource schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('datasource') if 'datasource' in kwargs else {}

    def set_type(self, type):
        self.raw['type'] = type
        return self

    def get_type(self):
        return self.raw['type'] if 'type' in self.raw else None

    def get_protocol(self):
        return self.raw['protocol'] if 'protocol' in self.raw else None

    def set_protocol(self, protocol):
        self.raw['protocol'] = protocol
        return self

    def get_host(self):
        return self.raw['host'] if 'host' in self.raw else None

    def set_host(self, host):
        self.raw['host'] = host
        return self

    def get_port(self):
        return self.raw['port'] if 'port' in self.raw else None

    def set_port(self, port):
        self.raw['port'] = port
        return self

    def get_username(self):
        return self.raw['username'] if 'username' in self.raw else None

    def set_username(self, username):
        self.raw['username'] = type
        return self

    def get_password(self):
        return self.raw['password'] if 'password' in self.raw else None

    def set_password(self, password):
        self.raw['password'] = type
        return self

    def to_json(self):
        return json.dumps(self.raw)
