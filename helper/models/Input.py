
import json


class Input:
    """Input schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('input') if 'input' in kwargs else {}

    def get_key(self):
        return self.raw['key'] if 'key' in self.raw else None

    def set_name(self, name):
        self.raw['name'] = name
        return self

    def get_name(self):
        return self.raw['name'] if 'name' in self.raw else None

    def set_value_type(self, stype):
        self.raw['valueType'] = {
            'type': stype
        }
        return self

    def get_value_type(self):
        return self.raw['valueType'] if 'valueType' in self.raw else None

    def set_event_type(self, stype):
        self.raw['eventType'] = {
            'type': stype
        }
        return self

    def get_event_type(self):
        return self.raw['eventType'] if 'eventType' in self.raw else None        

    def to_json(self):
        return json.dumps(self.raw)
