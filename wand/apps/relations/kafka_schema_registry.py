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
        self._get_all_tls_cert()


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
            enhanced_avro

    def set_schema_url(self, url):
        if not self.relation:
            return
        self.relation.data[self.unit]["url"] = url

    def set_client_auth(self, clientauth):
        if not self.relation:
            return
        self.relation.data[self.unit]["client_auth"] = self._clientauth


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
        return self.get_param("url")

    def get_param(self, param):
        if not self.relation:
            return None
        for u in self.relation.units:
            if param in self.relation.data[u]:
                return self.relation.data[u][param]
        return None
