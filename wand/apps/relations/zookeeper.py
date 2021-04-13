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

from ops.framework import Object
from ops.framework import StoredState

from charmhelpers.contrib.network.ip import get_hostname

from wand.security.ssl import CreateTruststore

__all__ = [
    'ZookeeperRelationMTLSNotSetError',
    'ZookeeperRelation',
    'ZookeeperProvidesRelation',
    'ZookeeperRequiresRelation'
]


class ZookeeperRelationMTLSNotSetError(Exception):

    def __init__(self, message="mTLS is configured on zookeeper"
                               " relation but not present on"
                               " configured on this unit."):
        super().__init__(self.message)


class ZookeeperRelation(Object):
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._charm = charm
        self._unit = charm.unit
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)
        self.state.set_default(zk_list="")
        # Space separated list of trusted_certs for mTLS
        self.state.set_default(mtls_trusted_certs="")
        self.state.set_default(mtls_ts_path="")
        self.state.set_default(mtls_ts_pwd="")

    @property
    def _relations(self):
        return self.framework.model.relations[self._relation_name]

    @property
    def get_zookeeper_list(self):
        return self.state.zk_list

    @property
    def advertise_addr(self):
        return self.model.get_binding(self._relation_name) \
               .network.ingress_address

    def _get_all_mtls_cert(self):
        if not self.is_mTLS_enabled():
            return
        self.state.trusted_certs = self.state.mtls_cert + " " + " ".join(
            [self._relation.data[u].get("mtls_cert", "")
             for u in self._relation.units])
        CreateTruststore(self.state.mtls_ts_path,
                         self.state.mtls_ts_pwd,
                         self.state.mtls_trusted_certs.split(),
                         ts_regenerate=True)

    # TODO(pguimaraes): complete this method. So far returns False as it is
    # not an option atm
    def is_sasl_enabled(self):
        return False

    def client_auth_enabled(self):
        return self.is_mTLS_enabled() or self.is_sasl_enabled()

    def is_mTLS_enabled(self):
        for u in self._relation.units:
            if self._relation.data[u].get("mtls_cert", None):
                # It is enabled, now we check
                # if we have it set this unit as well
                if not self._relation.data[self.unit].get("mtls_cert", None):
                    # we do not, so raise an error to inform it
                    raise ZookeeperRelationMTLSNotSetError()
                return True
        return False

    def set_mTLS_auth(self,
                      cert_chain,
                      truststore_path,
                      truststore_pwd):
        # 1) Publishes the cert on mtls_cert
        self._relation.data[self._unit]["mtls_cert"] = cert_chain
        self.state.mtls_ts_path = truststore_path
        self.state.mtls_ts_pwd = truststore_pwd
        self.state.mtls_trusted_certs = cert_chain
        self.state.mtls_cert = cert_chain
        # 2) Grab any already-published mtls certs and generate the truststore
        self._get_all_mtls_cert()

    def on_zookeeper_relation_joined(self, event):
        pass

    def on_zookeeper_relation_changed(self, event):
        zk_list = []
        for u in self._relation.units:
            if not self._relation.data[u]["endpoint"]:
                continue
            if len(self._relation.data[u]["endpoint"]) == 0:
                continue
            zk_list.append(self._relation.data[u]["endpoint"])
        self.state.zk_list = ",".join(zk_list)
        self._get_all_mtls_cert()


class ZookeeperProvidesRelation(ZookeeperRelation):

    def __init__(self, charm, relation_name, hostname=None, port=2182):
        super().__init__(charm, relation_name)
        self.framework.observe(charm.on.zookeeper_relation_changed,
                               self.on_zookeeper_relation_changed)
        self.framework.observe(charm.on.zookeeper_relation_joined,
                               self.on_zookeeper_relation_joined)
        self._hostname = hostname
        self._port = port

    def on_zookeeper_relation_joined(self, event):
        # Get unit's own hostname and pass that via relation
        hostname = self._hostname if self._hostname \
            else get_hostname(self.advertise_addr)
        self._relation.data[self._unit]["endpoint"] = \
            "{}:{}".format(hostname, self._port)

    def on_zookeeper_relation_changed(self, event):
        # First, update this unit entry for "endpoint"
        self.on_zookeper_relation_joined(event)
        # Second, recover data from peers
        # Third, check if mtls_cert has been set. If so, check
        # if any of the other peers have also published mtls for the
        # truststore.
        # This is done already on parent class method
        super().on_zookeeper_relation_changed(event)


# TODO(pguimaraes): Generate the: zookeeper-tls-client.properties.j2
class ZookeeperRequiresRelation(ZookeeperRelation):

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.framework.observe(charm.on.zookeeper_relation_changed, self)
