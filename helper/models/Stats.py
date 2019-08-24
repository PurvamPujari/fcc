
import json


class Stats:
    """Stats schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('stats') if 'stats' in kwargs else {}

    def get_data(self):
        return self.raw['data'] if 'data' in self.raw else None

    def set_data(self, data):
        self.raw['data'] = data
        return self

    def to_json(self):
        return json.dumps(self.raw)
