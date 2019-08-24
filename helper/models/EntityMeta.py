
import json


class EntityMeta:
    """EntityMeta schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('entityMeta') if 'entityMeta' in kwargs else {}

    def set_label(self, label):
        self.raw['label'] = label
        return self

    def get_label(self):
        return self.raw['label'] if 'label' in self.raw else None

    def get_id(self):
        return self.raw['id'] if 'id' in self.raw else None

    def get_sourceId(self):
        return self.raw['sourceId'] if 'sourceId' in self.raw else None

    def set_sourceId(self, sourceId):
        self.raw['sourceId'] = sourceId
        return self

    def get_path(self):
        return self.raw['path'] if 'path' in self.raw else None

    def set_path(self, path):
        self.raw['path'] = path
        return self

    def get_datastream(self):
        return self.raw['datastream'] if 'datastream' in self.raw else None

    def to_json(self):
        return json.dumps(self.raw)
