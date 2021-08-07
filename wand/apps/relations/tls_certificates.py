import json
import uuid

from ops.framework import Object

__all__ = [
    'TLSCertificateDataNotFoundInRelationError',
    'TLSCertificateRelationNotPresentError',
    'TLSCertificateRequiresRelation'
]


class TLSCertificateRelationNotPresentError(Exception):

    def __init__(self,
                 message="Certificate relation does not exist"):
        super().__init__(message)


class TLSCertificateDataNotFoundInRelationError(Exception):

    def __init__(self,
                 message="No certificate available for "
                         "this unit at the moment"):
        super().__init__(message)


class TLSCertificateRequiresRelation(Object):

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._unit = charm.unit
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)

    @property
    def unit(self):
        return self._unit

    @property
    def relation(self):
        return self.framework.model.get_relation(self._relation_name)

    @property
    def is_joined(self):
        return True if self.relation else False

    def get_chain(self):
        if not self.relation:
            return None
        chain = ""
        # We do not know which unit will contain the data
        # search until it finds out
        for u in self.relation.units:
            if "chain" in self.relation.data[u]:
                chain = self._process_cert(
                    self.relation.data[u]["chain"])
            if "ca" in self.relation.data[u]:
                chain += self._process_cert(
                    self.relation.data[u]["ca"])
                break
        return chain

    def request_client_cert(self, cn, sans):
        if not self.relation:
            return None
        to_publish_json = self.relation.data[self.unit]
        requests = to_publish_json.get('client_cert_requests', {})
        requests[cn] = {'sans': sans}
        to_publish_json['client_cert_requests'] = json.dumps(requests)

    def request_server_cert(self, cn, sans, cert_name=None):
        if not self.relation:
            return None
        to_publish_json = self.relation.data[self.unit]
        to_publish_raw = self.relation.data[self.unit]

        if 'common_name' not in to_publish_raw or \
           to_publish_raw['common_name'] in (None, '', cn):
            # for backwards compatibility, first request goes in its own fields
            to_publish_raw['common_name'] = cn
            to_publish_json['sans'] = json.dumps(sans or [])
            cert_name = to_publish_raw.get('certificate_name') or cert_name
            if cert_name is None:
                cert_name = str(uuid.uuid4())
            to_publish_raw['certificate_name'] = cert_name
        else:
            # subsequent requests go in the collection
            requests = to_publish_json.get('cert_requests', {})
            if isinstance(requests, str):
                requests = json.loads(requests)
            requests[cn] = {'sans': sans or []}
            to_publish_json['cert_requests'] = json.dumps(requests)

    def _process_cert(self, cert):
        begin = "-----BEGIN CERTIFICATE-----"
        end = "-----END CERTIFICATE-----"
        return begin + \
            cert.split(begin)[1].split(end)[0] + \
            end + "\n"

    def get_server_certs(self):
        if not self.relation:
            raise TLSCertificateRelationNotPresentError()
        name = self.unit.name.replace("/", "_")
        # Request not issued yet, therefore there is nothing to do
        if "common_name" not in self.relation.data[self.unit]:
            raise TLSCertificateDataNotFoundInRelationError()
        common_name = self.relation.data[self.unit]["common_name"]
        certs = {}
        # Search through the units until finding which is the easyrsa
        for u in self.relation.units:
            data = self.relation.data[u]
            if '{}.server.cert'.format(name) in data and \
               '{}.server.key'.format(name) in data:
                # First cert and key
                cert = data['{}.server.cert'.format(name)]
                key = data['{}.server.key'.format(name)]
                certs[common_name] = \
                    {"cert": self._process_cert(cert), "key": key}
                if '{}.processed_requests'.format(name) in data:
                    certs_data = \
                        data['{}.processed_requests'.format(name)] or {}
                    if isinstance(certs_data, str):
                        certs_data = json.loads(certs_data)
                    for k, v in certs_data.items():
                        certs[k] = {
                            "cert": self._process_cert(v["cert"]),
                            "key": v["key"]
                        }
                break
        if not certs:
            raise TLSCertificateDataNotFoundInRelationError()
        return certs

    def get_client_certs(self):
        if not self.relation:
            raise TLSCertificateRelationNotPresentError()
        name = self.unit.name.replace("/", "_")
        for u in self.relation.units:
            data = self.relation.data[u]
            field = '{}.processed_client_requests'.format(name)
            if field in data:
                certs_data = data[field] or {}
                break
        if not certs_data:
            raise TLSCertificateDataNotFoundInRelationError()
        return [{k: {"cert": v["cert"], "key": v["key"]}}
                for k, v in certs_data.items()]

    def on_tls_certificate_relation_joined(self, event):
        # Nothing to do so far
        pass

    def on_tls_certificate_relation_changed(self, event):
        pass
