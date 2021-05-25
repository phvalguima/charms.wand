from wand.contrib.linux import get_hostname
from wand.apps.relations.kafka_relation_base import KafkaRelationBase

__all__ = [
    "KafkaSRURLNotSetError",
    "KafkaSchemaRegistryRelation",
    "KafkaSchemaRegistryProvidesRelation",
    "KafkaSchemaRegistryRequiresRelation",
]


class KafkaSRURLNotSetError(Exception):

    def __init__(self,
                 message="Missing URL to connect to Schema Registry."):
        super().__init__(message)


class KafkaSchemaRegistryRelation(KafkaRelationBase):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, user, group, mode)

    @property
    def get_schema_url(self):
        if "url" not in self.relation.data[self.model.app]:
            raise KafkaSRURLNotSetError()
        return self.relation.data[self.model.app]["url"]

    def on_schema_registry_relation_joined(self, event):
        pass

    def on_schema_registry_relation_changed(self, event):
        pass


class KafkaSchemaRegistryProvidesRelation(KafkaSchemaRegistryRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, user, group, mode)
        self._clientauth = False

    def set_converter(self, converter):
        if not self.relation:
            return
        self.relation.data[self.unit]["converter"] = converter

    def set_enhanced_avro_support(self, enhanced_avro):
        if not self.relation:
            return
        self.relation.data[self.unit]["enhanced_avro"] = \
            str(enhanced_avro)

    def set_schema_url(self, url, port, prot):
        if not self.relations:
            return
        if not self.unit.is_leader():
            return
        # URL is set to config on schema registry, then the same value will
        # be passed by each of the schema registry instances. On the requester
        # side, the value collected is put on a set, which will end as one
        # single URL.
        for r in self.relations:
            r.data[self.model.app]["url"] = \
                "{}://{}:{}".format(
                    prot if len(prot) > 0 else "https",
                    url if len(url) > 0 else get_hostname(
                        self.advertise_addr),
                    port)

    def set_client_auth(self, clientauth):
        if not self.relation:
            return
        self.relation.data[self.unit]["client_auth"] = str(self._clientauth)


class KafkaSchemaRegistryRequiresRelation(KafkaSchemaRegistryRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, user, group, mode)

    @property
    def converter(self):
        return self.get_param("converter")

    @property
    def enhanced_avro(self):
        return self.get_param("enhanced_avro")

    @property
    def url(self):
        if not self.relations:
            return
        # Set is used to avoid repetitive URLs if schema_url config
        # is set instead of get_hostname of each advertise_addr
        for r in self.relations:
            if "url" in r.data[r.app]:
                return r.data[r.app]["url"]

    def get_param(self, param):
        if not self.relation:
            return None
        for u in self.relation.units:
            if param in self.relation.data[u]:
                return self.relation.data[u][param]
        return None

    def generate_configs(self,
                         ts_path,
                         ts_pwd,
                         enable_ssl,
                         ks_path,
                         ks_pwd):
        if not self.relations:
            return None
        sr_props = {}
        if len(ts_path) > 0:
            sr_props["schema.registry.ssl.truststore.location"] = ts_path
            sr_props["schema.registry.ssl.truststore.password"] = ts_pwd
        if enable_ssl:
            sr_props["schema.registry.ssl.key.password"] = ks_pwd
            sr_props["schema.registry.ssl.keystore.location"] = ks_path
            sr_props["schema.registry.ssl.keystore.password"] = ks_pwd
        sr_props["schema.registry.url"] = self.url
        return sr_props if len(sr_props) > 0 else None
