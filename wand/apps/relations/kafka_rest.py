#  Licensed under the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License. You may obtain
#  a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#  License for the specific language governing permissions and limitations
#  under the License.


"""
Implements the kafka REST proxy.
"""

from wand.apps.relations.kafka_relation_base import (
    KafkaRelationBase
)

__all__ = [
    "KafkaRESTRelation",
    "KafkaRESTProvidesRelation",
    "KafkaRESTRequiresRelation"
]


class KafkaRESTRelation(KafkaRelationBase):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8082, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode)
        self._hostname = hostname
        self._port = port
        self._protocol = protocol

    @property
    def url(self):
        if not self.relations:
            return None
        for r in self.relations:
            if "url" in r.data[self.model.app]:
                return r.data[self.model.app]["url"]

    @url.setter
    def url(self, u):
        if not self.relations:
            return
        for r in self.relations:
            r.data[self.model.app]["url"] = u


class KafkaRESTProvidesRelation(KafkaRESTRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8082, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode)


class KafkaRESTRequiresRelation(KafkaRESTRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8082, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode)
