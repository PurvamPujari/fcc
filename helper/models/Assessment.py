

import json


class Assessment:
    """Assessment schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('assessment') if 'assessment' in kwargs else {}

    def get_id(self):
        return self.raw['id'] if 'id' in self.raw else None

    def get_sourceId(self):
        return self.raw['sourceId'] if 'sourceId' in self.raw else None

    def set_name(self, name):
        self.raw['name'] = name
        return self

    def get_name(self):
        return self.raw['name'] if 'name' in self.raw else None

    def get_account(self):
        return self.raw['tenant'] if 'tenant' in self.raw else None

    def get_create_time(self):
        return self.raw['createTime'] if 'createTime' in self.raw else None

    def get_created_by(self):
        return self.raw['createdBy'] if 'createdBy' in self.raw else None

    def get_update_time(self):
        return self.raw['updateTime'] if 'updateTime' in self.raw else None

    def get_updated_by(self):
        return self.raw['updatedBy'] if 'updatedBy' in self.raw else None

    def get_datastream(self):
        return self.raw['datastream'] if 'datastream' in self.raw else None

    def get_rate(self):
        return self.raw['rate'] if 'rate' in self.raw else None

    def set_datastream(self, datastream):
        self.raw['datastream'] = datastream
        return self

    def get_live(self):
        return self.raw['live'] if 'live' in self.raw else None

    def to_json(self):
        return json.dumps(self.raw)

    def get_aprioriConditionList(self):
        return self.raw['aprioriConditionList'] if 'aprioriConditionList' in self.raw else []