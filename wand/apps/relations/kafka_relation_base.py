from ops.framework import Object
from ops.framework import StoredState

from wand.security.ssl import CreateTruststore

__all__ = [
    "KafkaRelationBaseTLSNotSetError",
    "KafkaRelationBase",
]


class KafkaRelationBaseTLSNotSetError(Exception):

    def __init__(self,
                 message="TLS detected on the relation but not on this unit"):
        super().__init__(message)


class KafkaRelationBase(Object):

    state = StoredState()

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name)

        self._charm = charm
        self._unit = charm.unit
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)
        # :: separated list of trusted_certs for TLS
        self.state.set_default(trusted_certs="")
        self.state.set_default(ts_path="")
        self.state.set_default(ts_pwd="")
        self.state.set_default(user=user)
        self.state.set_default(group=group)
        self.state.set_default(mode=mode)

    @property
    def user(self):
        return self.state.user

    @user.setter
    def user(self, x):
        self.state.user = x

    @property
    def group(self):
        return self.state.group

    @group.setter
    def group(self, x):
        self.state.group = x

    @property
    def mode(self):
        return self.state.mode

    @mode.setter
    def mode(self, x):
        self.state.mode = x

    @property
    def unit(self):
        return self._unit

    @property
    def relation(self):
        return self._relation

    @property
    def _relations(self):
        return self.framework.model.relations[self._relation_name]

    def _get_all_tls_cert(self):
        if not self.is_TLS_enabled():
            return
        self.state.trusted_certs = \
            "::".join(list(self.relation.data[u].get("tls_cert", "")
                           for u in self.relation.units))
        CreateTruststore(self.state.ts_path,
                         self.state.ts_pwd,
                         self.state.trusted_certs.split("::"),
                         ts_regenerate=True,
                         user=self.state.user,
                         group=self.state.group,
                         mode=self.state.mode)

    def is_TLS_enabled(self):
        for u in self.relation.units:
            if self.relation.data[u].get("tls_cert", None):
                # It is enabled, now we check
                # if we have it set this unit as well
                if not self.relation.data[self.unit].get("tls_cert", None):
                    # we do not, so raise an error to inform it
                    raise KafkaRelationBaseTLSNotSetError()
                return True
        return False

    def set_TLS_auth(self,
                     cert_chain,
                     truststore_path,
                     truststore_pwd):
        # 1) Publishes the cert on tls_cert
        self.relation.data[self.unit]["tls_cert"] = cert_chain
        self.state.ts_path = truststore_path
        self.state.ts_pwd = truststore_pwd
        self.state.trusted_certs = cert_chain
        # 2) Grab any already-published tls certs and generate the truststore
        self._get_all_tls_cert()
