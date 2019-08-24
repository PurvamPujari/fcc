

import jsonpickle
from falkonryclient.helper.models.Datasource import Datasource
from falkonryclient.helper.models.Input import Input
from falkonryclient.helper.models.Stats import Stats
from falkonryclient.helper.models.Field import Field

class Datastream:
    """Datastream schema class"""

    def __init__(self, **kwargs):
        self.raw = kwargs.get('datastream') if 'datastream' in kwargs else {}

        if 'field' in self.raw:
            field = self.raw['field']
            self.raw['field'] = Field(field=field) if 'field' in self.raw else None

        if 'datasource' in self.raw:
            datasource = self.raw['datasource']
            self.raw['datasource'] = Datasource(datasource=datasource) if 'datasource' in self.raw else None

        if 'stats' in self.raw:
            stats = self.raw['stats']
            self.raw['stats'] = Stats(stats=stats) if 'stats' in self.raw else None

        if 'inputList' in self.raw:
            if isinstance(self.raw['inputList'], list):
                inputs = []
                for input in self.raw['inputList']:
                    inputs.append(Input(input=input))
                self.raw['inputList'] = inputs

    def get_id(self):
        return self.raw['id'] if 'id' in self.raw else None

    def get_sourceId(self):
        return self.raw['sourceId'] if 'sourceId' in self.raw else None

    def set_name(self, name):
        self.raw['name'] = name
        return self

    def get_name(self):
        return self.raw['name'] if 'name' in self.raw else None

    def get_streaming(self):
        return self.raw['streaming'] if 'streaming' in self.raw else None

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

    def get_inputs(self):
        return self.raw['inputList'] if 'inputList' in self.raw else []

    def get_time_precision(self):
        return self.raw['timePrecision'] if 'timePrecision' in self.raw else None

    def set_time_precision(self, timePrecision):
        self.raw['timePrecision'] = timePrecision
        return self

    def set_inputs(self, inputs):
        input_list = []
        for input in inputs:
            if isinstance(input, Input):
                input_list.append(input)

        self.raw['inputList'] = input_list
        return self

    def set_datasource(self, datasource):
        if isinstance(datasource, Datasource):
            self.raw['datasource'] = datasource
        return self

    def get_datasource(self):
        return self.raw['datasource'] if 'datasource' in self.raw else None

    def set_stats(self, stats):
        if isinstance(stats, Stats):
            self.raw['stats'] = stats
        return self

    def get_stats(self):
        return self.raw['stats'] if 'stats' in self.raw else None

    def set_field(self, field):
        if isinstance(field, Field):
            self.raw['field'] = field
        return self

    def get_field(self):
        return self.raw['field'] if 'field' in self.raw else None


    def get_live(self):
        return self.raw['live'] if 'live' in self.raw else None

    def to_json(self):
        inputs = []
        for input in self.get_inputs():
            inputs.append(jsonpickle.unpickler.decode(input.to_json()))
        datastream = self.raw
        datastream['dataSource'] = jsonpickle.unpickler.decode((self.get_datasource()).to_json()) if self.get_datasource() is not None else None
        datastream['field'] = jsonpickle.unpickler.decode((self.get_field()).to_json()) if self.get_field() is not None else None
        datastream['stats'] = jsonpickle.unpickler.decode((self.get_stats()).to_json()) if self.get_stats() is not None else None
        datastream['inputList'] = inputs
        return jsonpickle.pickler.encode(datastream)