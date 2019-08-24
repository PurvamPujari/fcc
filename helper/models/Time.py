
import json


class Time:
    """Time schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('time') if 'time' in kwargs else {}


    def get_zone(self):
        return self.raw['zone'] if 'zone' in self.raw else None

    def set_zone(self, zone):
        self.raw['zone'] = zone
        return self

    def get_format(self):
        return self.raw['format'] if 'format' in self.raw else None

    def set_format(self, format):
        self.raw['format'] = format
        return self

    def get_identifier(self):
        return self.raw['identifier'] if 'identifier' in self.raw else None

    def set_identifier(self, identifier):
        self.raw['identifier'] = identifier
        return self

    def to_json(self):
        return json.dumps(self.raw)
