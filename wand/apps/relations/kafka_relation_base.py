from ops.framework import Object
from ops.framework import StoredState

from wand.security.ssl import CreateTruststore

__all__ = [
    "KafkaRelationBaseNotUsedError",
    "KafkaRelationBaseTLSNotSetError",
    "KafkaRelationBase",
]


class KafkaRelationBaseTLSNotSetError(Exception):

    def __init__(self,
                 message="TLS detected on the relation but not on this unit"):
        super().__init__(message)


class KafkaRelationBaseNotUsedError(Exception):
    def __init__(self,
                 message="There is no connections to this relation yet"):
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
    def charm(self):
        return self._charm

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

    def all_units(self, relation):
        u = relation.units.copy()
        if isinstance(u, set):
            u.add(self.unit)
        if isinstance(u, list):
            u.append(self.unit)
        return u

    @property
    def relations(self):
        return self.framework.model.relations[self._relation_name]

    def _get_all_tls_cert(self):
        if not self.is_TLS_enabled():
            # If there is no tls announced by relation peers,
            # then CreateTruststore is not needed. Do not raise an Exception
            # given it will use non-encrypted communication instead
            return
        crt_list = []
        for u in self.all_units(self.relation):
            if "tls_cert" in self.relation.data[u]:
                crt_list.append(self.relation.data[u]["tls_cert"])
        self.state.trusted_certs = "::".join(crt_list)
        CreateTruststore(self.state.ts_path,
                         self.state.ts_pwd,
                         self.state.trusted_certs.split("::"),
                         ts_regenerate=True,
                         user=self.state.user,
                         group=self.state.group,
                         mode=self.state.mode)

    def is_TLS_enabled(self):
        if not self.relations:
            raise KafkaRelationBaseNotUsedError()
        for r in self.relations:
            for u in self.all_units(r):
                if r.data[u].get("tls_cert", None):
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
        if not self.relations:
            raise KafkaRelationBaseNotUsedError()
        for r in self.relations:
            # 1) Publishes the cert on tls_cert
            r.data[self.unit]["tls_cert"] = cert_chain
        self.state.ts_path = truststore_path
        self.state.ts_pwd = truststore_pwd
        self.state.trusted_certs = cert_chain
        # 2) Grab any already-published tls certs and generate the truststore
        self._get_all_tls_cert()

    @property
    def peer_addresses(self):
        addresses = []
        for u in self.relation.units:
            addresses.append(self.relation.data[u]["ingress-address"])
        return addresses

    @property
    def advertise_addr(self):
        m = self.model
        return str(m.get_binding(self._relation_name).network.ingress_address)

    @property
    def binding_addr(self):
        m = self.model
        return str(m.get_binding(self._relation_name).network.bind_address)
