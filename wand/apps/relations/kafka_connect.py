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


class KafkaConnectRelation(KafkaRelationBase):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, user, group, mode)

    def on_connect_relation_joined(self, event):
        pass

    def on_connect_relation_changed(self, event):
        pass


class KafkaConnectProvidesRelation(KafkaConnectRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, user, group, mode)

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
