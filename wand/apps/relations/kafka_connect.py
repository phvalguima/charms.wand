from wand.apps.relations.kafka_relation_base import (
    KafkaRelationBase,
    KafkaRelationBaseNotUsedError
)

__all__ = [
    "KafkaConnectRelationNotUsedError",
    "KafkaConnectRelation",
    "KafkaConnectProvidesRelation",
    "KafkaConnectRequiresRelation"
]


class KafkaConnectRelationNotUsedError(Exception):

    def __init__(self,
                 message="Connect relation not set."):
        super().__init__(message)
        self.state.set_default(url="")


class KafkaConnectRelation(KafkaRelationBase):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0, hostname=None):
        super().__init__(charm, relation_name, user, group, mode)

    @property
    def url(self):
        return self.state.url

    @url.setter
    def url(self, u):
        self.state.url = u

    def on_connect_relation_joined(self, event):
        pass

    def on_connect_relation_changed(self, event):
        pass


class KafkaConnectProvidesRelation(KafkaConnectRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0, hostname=None):
        super().__init__(charm, relation_name, user, group, mode,
                         hostname)

    @property
    def url(self):
        if not self.relations:
            return None
        for r in self.relations:
            if len(r.data[self.model.app].get("url", "")) > 0:
                return r.data[self.model.app]["url"]

    @url.setter
    def url(self, u):
        self.state.url = u
        if not self.unit.is_leader():
            return
        if not self.relations:
            return
        for r in self.relations:
            if u != r.data[self.model.app].get("url", ""):
                r.data[self.model.app]["url"] = u

    def set_TLS_auth(self,
                     cert_chain,
                     truststore_path,
                     truststore_pwd,
                     user=None,
                     group=None,
                     mode=None,
                     extra_certs=[]):
        try:
            super().set_TLS_auth(
                cert_chain,
                truststore_path,
                truststore_pwd,
                user=user,
                group=group,
                mode=mode,
                extra_certs=extra_certs)
        except KafkaRelationBaseNotUsedError:
            # Capture the generic error and raise an specific for this class
            raise KafkaConnectRelationNotUsedError()


class KafkaConnectRequiresRelation(KafkaConnectRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, user, group, mode)

    @property
    def url(self):
        if not self.relations:
            return None
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
        props[prefix + "cluster"] = self.url
        if len(ts_path) > 0:
            props[prefix + "ssl.truststore.location"] = ts_path
            props[prefix + "ssl.truststore.password"] = ts_pwd
        if len(ts_path) > 0 and enable_keystore:
            props[prefix + "ssl.key.password"] = ks_pwd
            props[prefix + "ssl.keystore.password"] = ks_pwd
            props[prefix + "ssl.keystore.location"] = ks_path
        return props if len(props) > 0 else None
