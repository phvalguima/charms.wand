from wand.apps.relations import KafkaRelationBase

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

    @property
    def set_schema_url(self, url):
        self.relation.data[self.model.app]["url"] = url


class KafkaSchemaRegistryRequiresRelation(KafkaSchemaRegistryRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, user, group, mode)
