import json


class Tracker:
    """Tracker schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('tracker') if 'tracker' in kwargs else {}

    def set_status(self, status):
        self.raw['status'] = status
        return self

    def get_status(self):
        return self.raw['status'] if 'status' in self.raw else None

    def get_id(self):
        return self.raw['__id'] if '__id' in self.raw else None

    def set_id(self, id):
        self.raw['__id'] = id
        return self

    def get_action(self):
        return self.raw['action'] if 'action' in self.raw else None

    def set_action(self, action):
        self.raw['action'] = action
        return self

    def to_json(self):
        return json.dumps(self.raw)
