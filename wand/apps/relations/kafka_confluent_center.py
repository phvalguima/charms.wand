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
Implements the kafka Confluent Control Center relation.
"""


from wand.apps.relations.kafka_relation_base import (
    KafkaRelationBase
)

__all__ = [
    "KafkaC3Relation",
    "KafkaC3ProvidesRelation",
    "KafkaC3RequiresRelation"
]


class KafkaC3Relation(KafkaRelationBase):

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


class KafkaC3ProvidesRelation(KafkaC3Relation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8082, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode)


class KafkaC3RequiresRelation(KafkaC3Relation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8082, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode)

    def generate_config(self,
                        mds_bootstrap_servers,
                        ts_path,
                        ts_pwd,
                        oauthbearer_settings,
                        client_security_protocol,
                        sasl_oauthbearer_enabled=False):
        if not self.c3.relations:
            return
        props = {}
        props["confluent.monitoring"
              ".interceptor.bootstrap.servers"] = mds_bootstrap_servers
        props["confluent.monitoring.interceptor.topic"] = \
            "_confluent-monitoring"
        props["consumer.interceptor.classes"] = \
            "io.confluent.monitoring.clients.interceptor" + \
            ".MonitoringConsumerInterceptor"
        props["producer.interceptor.classes"] = \
            "io.confluent.monitoring.clients" + \
            ".interceptor.MonitoringProducerInterceptor"
        if len(ts_path) > 0:
            props["client.confluent.monitoring"
                  ".interceptor.ssl.truststore.location"] = ts_path
            props["client.confluent.monitoring"
                  ".interceptor.ssl.truststore.password"] = ts_pwd
        if sasl_oauthbearer_enabled:
            props["client.confluent.monitoring."
                  "interceptor.sasl.jaas.config"] = oauthbearer_settings
            props["client.confluent.monitoring"
                  ".interceptor.sasl.mechanism=OAUTHBEARER"] = \
                "OAUTHBEARER"
            props["client.confluent.monitoring"
                  ".interceptor.security.protocol"] = \
                client_security_protocol
            props["client.confluent.monitoring.interceptor"
                  ".sasl.login.callback.handler.class"] = \
                "io.confluent.kafka.clients.plugins.auth.token." + \
                "TokenUserLoginCallbackHandler"
