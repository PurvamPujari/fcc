"""
Falkonry Client

Client to access Condition Prediction APIs

:copyright: (c) 2016-2018 by Falkonry Inc.
:license: MIT, see LICENSE for more details.

"""

from falkonryclient.helper.schema import Models

"""
EntityService class :
Class to handle and validate entities and their schemas

Note: Unimplemented
"""


class EntityService:

    def __init__(self):
        """empty"""

    def get_schema(self, entity_name):
        """
        To get the schema of the requested entity name
        :param entity_name: string
        """
        return Models[entity_name]

    def validate(self, object):
        """
        To validate structure of the requested entity
        :param object: json
        """
        return True
