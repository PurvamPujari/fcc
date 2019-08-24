
import json


class Signal:
    """Signal schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('signal') if 'signal' in kwargs else {}

    def get_signalIdentifier(self):
        return self.raw['signalIdentifier'] if 'signalIdentifier' in self.raw else None

    def set_signalIdentifier(self, signalIdentifier):
        self.raw['signalIdentifier'] = signalIdentifier
        return self


    def get_valueIdentifier(self):
        return self.raw['valueIdentifier'] if 'valueIdentifier' in self.raw else None

    def set_valueIdentifier(self, valueIdentifier):
        self.raw['valueIdentifier'] = valueIdentifier
        return self


    def to_json(self):
        return json.dumps(self.raw)
