

import json


class AssessmentRequest:
    """AssessmentRequest schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('assessmentRequest') if 'assessmentRequest' in kwargs else {}

    def set_name(self, name):
        self.raw['name'] = name
        return self

    def get_name(self):
        return self.raw['name'] if 'name' in self.raw else None

    def get_datastream(self):
        return self.raw['datastream'] if 'datastream' in self.raw else None

    def set_datastream(self, datastream):
        self.raw['datastream'] = datastream
        return self

    def get_rate(self):
        return self.raw['rate'] if 'rate' in self.raw else None

    def set_rate(self, rate):
        self.raw['rate'] = rate
        return self

    def to_json(self):
        return json.dumps(self.raw)
