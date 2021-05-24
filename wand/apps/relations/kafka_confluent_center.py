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
Implements the Confluent Control Center relation.

This relation passes on the listener configuration for the interceptor
logic implemented on confluent stack.

The provider side needs to supply the listener endpoint to be used.
That differs from traditional listeners given that the same listener
will be used by all the requirers.

The provider side should call:
    self.c3 = KafkaC3ProvidesRelation(...)
    self.c3.url = "<hostname>:<listener-port>"
    ...

This relation and listeners should never be on different spaces.

The requirer side will have access to the listener bootstrap urls
and should check which SASL mechanism was added.

The requirer side should use the same truststore and keystore as
the ones to be used in the listener relation.

TODO: the SASL mechanism is hard-coded to be OAUTHBEARER using
LDAP credentials. That has been hardcoded on the Requirer side.
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
            if "bootstrap-server" in r.data[self.unit]:
                return r.data[self.unit]["bootstrap-server"]

    @url.setter
    def url(self, u):
        if not self.relations:
            return
        for r in self.relations:
            r.data[self.unit]["bootstrap-server"] = u


class KafkaC3ProvidesRelation(KafkaC3Relation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8082, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode)
        self.state.set_default(listener="{}")


class KafkaC3RequiresRelation(KafkaC3Relation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8082, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode)

    def get_bootstrap_servers(self):
        if not self.relations:
            return
        servers = []
        for r in self.relations:
            for u in r.units:
                if "bootstrap-server" in r.data[u]:
                    servers.append(r.data[u]["bootstrap-server"])
        return ",".join(servers)

    def generate_configs(self,
                         ts_path,
                         ts_pwd,
                         oauthbearer_settings,
                         client_security_protocol,
                         sasl_oauthbearer_enabled=False):
        if not self.relations:
            return
        props = {}
        props["confluent.monitoring"
              ".interceptor.bootstrap.servers"] = \
            self.get_bootstrap_servers()
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
                  ".interceptor.sasl.mechanism"] = \
                "OAUTHBEARER"
            props["client.confluent.monitoring"
                  ".interceptor.security.protocol"] = \
                client_security_protocol
            props["client.confluent.monitoring.interceptor"
                  ".sasl.login.callback.handler.class"] = \
                "io.confluent.kafka.clients.plugins.auth.token." + \
                "TokenUserLoginCallbackHandler"
        return props if len(props) > 0 else None
