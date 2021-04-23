from wand.apps.relations.kafka_relation_base import (
    KafkaRelationBase,
    KafkaRelationBaseNotUsedError
)
from wand.contrib.linux import get_hostname

from wand.security.ssl import genRandomPassword

__all__ = [
    "KafkaMDSRelation",
    "KafkaMDSProvidesRelation",
    "KafkaMDSRequiresRelation",
]


# MDS relation only exists to exchange the MDS endpoint data
# Therefore, there is no need for certificate-related logic
class KafkaMDSRelation(KafkaRelationBase):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8090, protocol="https"):
        super().__init__(charm, relation_name, user, group, mode)
        self._charm = charm
        self._unit = charm.unit
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(
            self._relation_name)
        self._hostname = hostname
        self._port = port
        self._protocol = protocol
        self.state.set_default(mds_list="")
        self.state.set_default(mds_url="")

    # No setter because only mds_url should not be changed for
    # KafkaMDSRequiresRelation
    @property
    def mds_url(self):
        return self.state.mds_url

    @mds_url.setter
    def mds_url(self, x):
        self.state.mds_url = x

    @property
    def hostname(self):
        return self._hostname

    @property
    def port(self):
        return self._port

    @property
    def protocol(self):
        return self._protocol

    @hostname.setter
    def hostname(self, x):
        self._hostname = x

    @port.setter
    def port(self, x):
        self._port = x

    @protocol.setter
    def protocol(self, x):
        self._protocol = x

    def get_mds_server_list(self):
        return self.state.mds_list

    def on_mds_relation_joined(self, event):
        pass

    def on_mds_relation_changed(self, event):
        pass


class KafkaMDSProvidesRelation(KafkaMDSRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8090, protocol="https"):
        super().__init__(charm, relation_name, user, group, mode,
                         hostname, port, protocol)

    def unset_auth(self):
        if not self.unit.is_leader or not self.relations:
            return
        for r in self.relations:
            for u in r.units:
                if u.name.startswith(self.unit.app.name):
                    # Units of the same application
                    continue
                r.data[u].pop("user", None)
                r.data[u].pop("password", None)
                r.data[u].pop("auth_mechanism", None)
                r.data[u].pop("cred_provider", None)

    def set_auth(self, cred_provider="BASIC", auth_mech="JETTY_AUTH"):
        if not self.unit.is_leader or not self.relations:
            return
        for r in self.relations:
            for u in r.units:
                if u.name.startswith(self.unit.app.name):
                    # Units of the same application
                    continue
                r.data[u]["user"] = u.name.replace("/", "_")
                r.data[u]["password"] = genRandomPassword(12)
                r.data[u]["auth_mechanism"] = "JETTY_AUTH"
                r.data[u]["cred_provider"] = "BASIC"

    def on_mds_relation_joined(self, event):
        return

    def on_mds_relation_changed(self, event):
        r = event.relation
        hostname = self._hostname if self._hostname \
            else get_hostname(self.binding_addr)
        r.data[self.unit]["url"] = \
            "{}://{}:{}".format(self._protocol,
                                hostname,
                                self._port)


class KafkaMDSRequiresRelation(KafkaMDSRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8090, protocol="https"):
        super().__init__(charm, relation_name, user, group, mode,
                         hostname, port, protocol)

    def get_mds_user(self):
        if not self.relations():
            raise KafkaRelationBaseNotUsedError()
        for r in self.relations:
            for u in r.units:
                if "user" in r.data[u]:
                    return r.data[u]["user"]

    def get_mds_password(self):
        if not self.relations:
            raise KafkaRelationBaseNotUsedError()
        for r in self.relations:
            for u in r.units:
                if "password" in r.data[u]:
                    return r.data[u]["password"]

    def get_mds_server_list(self):
        mds_list = []
        if not self.relations:
            raise KafkaRelationBaseNotUsedError()
        for r in self.relations:
            for u in r.units:
                if "url" in r.data[u]:
                    mds_list.append(r.data[u]["url"])
        return ",".join(mds_list)

    def get_auth_mech(self):
        if not self.relation:
            raise KafkaRelationBaseNotUsedError()
        return self.relation[self.unit].get("auth_mechanism", "BASIC")

    def get_cred_provider(self):
        if not self.relation:
            raise KafkaRelationBaseNotUsedError()
        return self.relation[self.unit].get("cred_provider", "JETTY_AUTH")

    def is_auth_set(self):
        if not self.relation:
            raise KafkaRelationBaseNotUsedError()
        if "user" in self.relation[self.unit] and \
                "password" in self.relation[self.unit]:
            return True
        return False

    def get_authorizer(self):
        if not self.relation:
            raise KafkaRelationBaseNotUsedError()
        return self.relation[self.unit].get(
            "authorizer",
            "io.confluent.kafka.schemaregistry."
            "security.authorizer.rbac.RbacAuthorizer")

    def get_options(self):
        props = {}
        props["confluent.metadata.bootstrap.server.urls"] = \
            self.get_server_list()
        if self.is_auth_set():
            props["confluent.metadata.basic.auth.user.info"] = \
                "{}:{}".format(
                    self.get_mds_user(),
                    self.get_mds_password())
        props["confluent.metadata.http.auth.credentials"
              ".provider"] = self.get_cred_provider()
        props["confluent.schema.registry.auth"
              ".mechanism"] = self.get_auth_mech()
        props["confluent.schema.registry.authorizer"
              ".class"] = self.get_authorizer()
        return props

    def on_mds_relation_joined(self, event):
        pass

    def on_mds_relation_changed(self, event):
        pass
