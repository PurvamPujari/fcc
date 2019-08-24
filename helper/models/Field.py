

import json
import jsonpickle
from falkonryclient.helper.models.Signal import Signal
from falkonryclient.helper.models.Time import Time


class Field:
    """Field schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('field') if 'field' in kwargs else {}
        if 'time' in self.raw:
            time = self.raw['time']
            self.raw['time'] = Time(time=time) if 'time' in self.raw else None

        if 'signal' in self.raw:
            signal = self.raw['signal']
            self.raw['signal'] = Signal(signal=signal) if 'signal' in self.raw else None


    def set_entityIdentifier(self, entityIdentifier):
        self.raw['entityIdentifier'] = entityIdentifier
        return self

    def get_entityIdentifier(self):
        return self.raw['entityIdentifier'] if 'entityIdentifier' in self.raw else None

    def set_entityName(self, entityName):
        self.raw['entityName'] = entityName
        return self

    def get_entityName(self):
        return self.raw['entityName'] if 'entityName' in self.raw else None

    def set_signal(self, signal):
        if isinstance(signal, Signal):
            self.raw['signal'] = signal
        return self

    def get_signal(self):
        return self.raw['signal'] if 'signal' in self.raw else None

    def set_time(self, time):
        if isinstance(time, Time):
            self.raw['time'] = time
        return self

    def get_time(self):
        return self.raw['time'] if 'time' in self.raw else None

    def to_json(self):
        field = self.raw;
        field['time'] = jsonpickle.unpickler.decode((self.get_time()).to_json())
        field['signal'] = jsonpickle.unpickler.decode((self.get_signal()).to_json())
        return json.dumps(field)

    def set_batchIdentifier(self, batchIdentifier):
        self.raw['batchIdentifier'] = batchIdentifier
        return self

    def get_batchIdentifier(self):
        return self.raw['batchIdentifier'] if 'batchIdentifier' in self.raw else None
