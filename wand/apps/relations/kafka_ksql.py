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
Implements the kafka KSQL.
"""

from wand.apps.relations.kafka_relation_base import (
    KafkaRelationBase
)

__all__ = [
    "KafkaKsqlRelation",
    "KafkaKsqlProvidesRelation",
    "KafkaKsqlRequiresRelation"
]


class KafkaKsqlRelation(KafkaRelationBase):

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
            if r.data[r.app].get("url", "") != u:
                r.data[self.model.app]["url"] = u


class KafkaKsqlProvidesRelation(KafkaKsqlRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8082, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode)


class KafkaKsqlRequiresRelation(KafkaKsqlRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8082, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode)

    @property
    def url(self):
        if not self.relations:
            return
        for r in self.relations:
            if len(r.data[r.app].get("url", "")) > 0:
                return r.data[r.app]["url"]

    def generate_configs(self,
                         ts_path,
                         ts_pwd,
                         enable_keystore,
                         ks_path,
                         ks_pwd,
                         prefix=""):
        if not self.relations:
            return
        props = {}
        props[prefix + "advertised.url"] = self.url
        if len(ts_path) > 0:
            props[prefix + "ssl.truststore.location"] = ts_path
            props[prefix + "ssl.truststore.password"] = ts_pwd
        if len(ts_path) > 0 and enable_keystore:
            props[prefix + "ssl.key.password"] = ks_pwd
            props[prefix + "ssl.keystore.password"] = ks_pwd
            props[prefix + "ssl.keystore.location"] = ks_path
        return props if len(props) > 0 else None
