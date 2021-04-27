import json
import logging

from wand.security.ssl import genRandomPassword
from wand.apps.relations.kafka_relation_base import KafkaRelationBase
from wand.contrib.linux import get_hostname

logger = logging.getLogger(__name__)

__all__ = [
    "KafkaListenerRelation",
    "KafkaListenerProvidesRelation",
    "KafkaListenerRequiresRelation",
    "KafkaListenerRelationNotSetError",
    "KafkaListenerRelationEmptyListenerDictError"
]


class KafkaListenerRelationEmptyListenerDictError(Exception):
    def __init__(self,
                 message="Empty Listener dict provided"):
        super().__init__(message)


class KafkaListenerRelationNotSetError(Exception):
    def __init__(self,
                 message="Relation not ready, waiting for connection"):
        super().__init__(message)


class KafkaListenerRelation(KafkaRelationBase):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=443, protocol="SSL"):
        super().__init__(charm, relation_name, user, group, mode)
        self._hostname = hostname
        self._port = port
        self._protocol = protocol

    @property
    def hostname(self):
        return self._hostname if self._hostname \
            else get_hostname(self.binding_addr)

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

    def on_listener_relation_joined(self, event):
        pass

    def on_listener_relation_changed(self, event):
        pass


# Provider side is run by kafka broker: it publishes its endpoint for a given
# client to use it. On the other side, it receives a dict describing what the
# other application waits as listener (SASL or not, e.g. kerberos) or SSL.
class KafkaListenerProvidesRelation(KafkaListenerRelation):

    # Ports 9092-9094 reserved for internal, broker and external
    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=9092, protocol="SSL"):
        super().__init__(charm, relation_name, user, group, mode,
                         hostname, port, protocol)
        self.state.set_default(available_port=port)
        self.state.set_default(internal_pwd=genRandomPassword(12))
        self.state.set_default(external_pwd=genRandomPassword(12))
        self.state.set_default(broker_pwd=genRandomPassword(12))

    def _get_default_listeners(self, keystore_path, keystore_pwd, clientauth):
        listeners = {
            "internal": {
                "endpoint": "INTERNAL://*BINDING*:{}".format(
                    self.state.available_port),
                "advertise": "INTERNAL://*BINDING*:{}".format(
                    self.state.available_port),
                "plaintext_pwd": self.state.internal_pwd,
                "cert_present": True,
                "sasl_present": False,
                "secprot": "SSL",
                "ts_path": self.state.ts_path,
                "ts_pwd": self.state.ts_pwd,
                "ks_path": keystore_path,
                "ks_pwd": keystore_pwd
            },
            "external": {
                "endpoint": "EXTERNAL://*ADVERTISE*:{}".format(
                    self.state.available_port + 1),
                "advertise": "EXTERNAL://*ADVERTISE*:{}".format(
                    self.state.available_port + 1),
                "plaintext_pwd": self.state.external_pwd,
                "cert_present": True,
                "sasl_present": False,
                "secprot": "SSL",
                "ts_path": self.state.ts_path,
                "ts_pwd": self.state.ts_pwd,
                "ks_path": keystore_path,
                "ks_pwd": keystore_pwd
            },
            "broker": {
                "endpoint": "BROKER://*BINDING*:{}".format(
                    self.state.available_port + 2),
                "advertise": "BROKER://*BINDING*:{}".format(
                    self.state.available_port + 2),
                "plaintext_pwd": self.state.broker_pwd,
                "cert_present": True,
                "sasl_present": False,
                "secprot": "SSL",
                "ts_path": self.state.ts_path,
                "ts_pwd": self.state.ts_pwd,
                "ks_path": keystore_path,
                "ks_pwd": keystore_pwd
            }
        }
        return listeners

    # Only the leader runs this method.
    # Generates a string with the template dict
    # That template dict contains entries that should be replaced
    # on each node: *BINDING* and *ADVERTISE* for its respective
    # binding and advertise addresses or hostnames.
    def get_unit_listener(self,
                          keystore_path,
                          keystore_pwd,
                          get_default=True,
                          clientauth=False):
        self.state.available_port = self.port
        if not self.relations:
            raise KafkaListenerRelationNotSetError()
        listeners = {}
        if get_default:
            listeners = self._get_default_listeners(
                keystore_path, keystore_pwd, clientauth)
        if not self.unit.is_leader():
            return None

        # Leader sets the value
        if get_default:
            listeners = self._get_default_listeners(
                keystore_path, keystore_pwd, clientauth)
        # Consider the 3 ports to be used if default is enabled
        self.state.available_port += 3
        for r in self.relations:
            for u in r.units:
                if "request" in r.data[u]:
                    inter = r.data[u]["request"]
                else:
                    continue
                req = json.loads(inter)
                if not req:
                    # for the case req = {}
                    continue
                # Jump to the next port available
                self.state.available_port += 1
                listener_name = u.app.name.replace("-", "_")
                addr = None
                if req.get("is_public", False):
                    addr = "*ADVERTISE*"
                else:
                    addr = "*BINDING*"
                lt = listener_name + \
                    "://" + addr + ":" + \
                    str(self.state.available_port)
                listeners[listener_name] = {}
                listeners[listener_name]["bootstrap_server"] = \
                    addr + ":" + \
                    str(self.state.available_port)
                listeners[listener_name]["endpoint"] = lt
                listeners[listener_name]["advertise"] = lt
                cert_present = "cert" in req
                sasl_present = "sasl" in req
                if not cert_present and not sasl_present:
                    listeners[listener_name]["secprot"] = "PLAINTEXT"
                elif cert_present and not sasl_present:
                    listeners[listener_name]["secprot"] = "SSL"
                if not cert_present and sasl_present:
                    listeners[listener_name]["secprot"] = "SASL"
                elif cert_present and sasl_present:
                    listeners[listener_name]["secprot"] = "SASL_SSL"
                # TODO: dict to the SASL part, where the root is
                # the type of SASL
                listeners[listener_name]["SASL"] = {}
                listeners[listener_name]["plaintext_pwd"] = \
                    req["plaintext_pwd"]
                listeners[listener_name]["cert_present"] = cert_present
                listeners[listener_name]["sasl_present"] = sasl_present
                listeners[listener_name]["ts_path"] = self.state.ts_path
                listeners[listener_name]["ts_pwd"] = self.state.ts_pwd
                listeners[listener_name]["ks_path"] = keystore_path
                listeners[listener_name]["ks_pwd"] = keystore_pwd
                listeners[listener_name]["clientauth"] = clientauth
        # update all the units
        return json.dumps(listeners)

    def _convert_listener_template(self, lst):
        if not lst or len(lst) == 0:
            raise KafkaListenerRelationEmptyListenerDictError()
        listeners = lst.replace(
            "*BINDING*", get_hostname(self.binding_addr))
        listeners = listeners.replace(
            "*ADVERTISE*", get_hostname(self.advertise_addr))
        listeners = json.loads(listeners)
        return listeners

    def _generate_opts(self, _lst,
                       keystore_path,
                       keystore_pwd,
                       get_default=True,
                       clientauth=False):
        if not _lst:
            raise KafkaListenerRelationEmptyListenerDictError()
        # In case _lst comes as None
        lst = _lst or "{}"
        # Convert the template
        listeners = self._convert_listener_template(lst)
        # Now set the options
        listener_opts = {}
        listener_opts["listeners"] = ",".join(
            [l["endpoint"]
             for k, l in listeners.items()])
        listener_opts["advertised.listeners"] = ",".join(
            [l["advertise"]
             for k, l in listeners.items()])
        listener_opts["listener.security.protocol.map"] = ",".join(
            ["{}:{}".format(k, l["secprot"]) for k, l in listeners.items()])
        prefix = "listener.name."
        for k, v in listeners.items():
            if v["sasl_present"]:
                # TODO: implement the logic below
                listener_opts[prefix + k + ".gssapi.sasl.jaas.config"] = None
                listener_opts[prefix + k + ".sasl.enabled.mechanisms"] = None
                listener_opts[prefix +
                              k + ".sasl.kerberos.service.name"] = None
            if v["cert_present"]:
                listener_opts[prefix +
                              k + ".ssl.client.auth"] = \
                                  "required" if clientauth else "none"
                listener_opts[prefix +
                              k + ".ssl.key.password"] = keystore_pwd
                listener_opts[prefix +
                              k + ".ssl.keystore.location"] = keystore_path
                listener_opts[prefix +
                              k + ".ssl.keystore.password"] = keystore_pwd
                if len(self.state.ts_path) > 0:
                    listener_opts[prefix + k + ".ssl.truststore.location"] = \
                        self.state.ts_path
                    listener_opts[prefix + k + ".ssl.truststore.password"] = \
                        self.state.ts_pwd
        return listener_opts

    def on_listener_relation_joined(self, event):
        # There is nothing to do at first from kafka broker perspective.
        # Each charm will publish at -joined event what it is looking for:
        # PLAINTEXT, SSL, SASL_SSL, etc and if this is a public or
        # private link.
        return

    def on_listener_relation_changed(self, event):
        self._get_all_tls_cert()
        return
#        listener, _ = self.get_unit_listener(
#            keystore_path="",
#            keystore_pwd="",
#            get_default=False,
#            clientauth=False)

    def set_bootstrap_data(self, lst):
        if not lst or len(lst) == 0:
            raise KafkaListenerRelationEmptyListenerDictError()
        if not self.relations:
            return
        for r in self.relations:
            listener = self._convert_listener_template(lst)
            for k, v in listener.items():
                data = {}
                # Make a copy, so we can change its content
                data[k] = dict(listener[k])
                if "ts_pwd" in data[k]:
                    del data[k]["ts_pwd"]
                if "ks_pwd" in data[k]:
                    del data[k]["ks_pwd"]
            logger.debug("Listeners: set_bootstrap_data={}".format(listener))
            j = json.dumps(listener)
            if j != r.data[self.unit].get("bootstrap-server", ""):
                r.data[self.unit]["bootstrap-server"] = j


# Requirer is run on the charm clients connecting to kafka brokers
class KafkaListenerRequiresRelation(KafkaListenerRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, user, group, mode)
        self.state.set_default(is_public=False)
        self.state.set_default(sasl="")
        self.state.set_default(request="{}")
        self.set_plaintext_pwd(genRandomPassword())

    def set_plaintext_pwd(self, pwd):
        req = json.loads(self.state.request) or {}
        # changing relation data will trigger a -changed event on
        # the Provides side, which also triggers a relation data update.
        # That can lead to an infinite loop of changes.
        if "plaintext_pwd" in req:
            if pwd == req["plaintext_pwd"]:
                return
        req["plaintext_pwd"] = pwd
        self.state.request = json.dumps(req)
        self.set_request(req)

    def set_sasl(self, sasl):
        req = json.loads(self.state.request) or {}
        # changing relation data will trigger a -changed event on
        # the Provides side, which also triggers a relation data update.
        # That can lead to an infinite loop of changes.
        if "SASL" in req:
            if sasl == req["SASL"]:
                return
        req["SASL"] = sasl
        self.state.request = json.dumps(req)
        self.set_request(req)

    def set_is_public(self, is_public):
        req = json.loads(self.state.request) or {}
        # changing relation data will trigger a -changed event on
        # the Provides side, which also triggers a relation data update.
        # That can lead to an infinite loop of changes.
        if "is_public" in req:
            if is_public == req["is_public"]:
                return
        req["is_public"] = is_public
        self.state.request = json.dumps(req)
        self.set_request(req)

    def tls_client_auth_enabled(self):
        for r in self.relations:
            for u in r.units:
                if "clientauth" in r.data[u]:
                    return r.data[u]["clientauth"]
        return False

    def set_request(self, req):
        j = json.dumps(req)
        # changing relation data will trigger a -changed event on
        # the Provides side, which also triggers a relation data update.
        # That can lead to an infinite loop of changes.
        if j == self.state.request:
            return
        self.state.request = j
        self._set_request()

    def _set_request(self):
        if not self.relations:
            return
        for r in self.relations:
            r.data[self.unit]["request"] = self.state.request

    def get_bootstrap_servers(self):
        if not self.relations:
            raise KafkaListenerRelationNotSetError()
        # The Requires side published its request on the relation
        # Kafka-broker generated the listener map based on the request
        # Pushes back the available listeners and the Requires side can build
        # its bootstrap-servers option
        servers = []
        lst_name = self.unit.app.name.replace("-", "_")
        for r in self.relations:
            for u in r.units:
                if "bootstrap-server" in r.data[u]:
                    req = json.loads(r.data[u]["bootstrap-server"])
                    try:
                        endpoint = \
                            req[lst_name]["bootstrap_server"]
                    except KeyError:
                        raise KafkaListenerRelationNotSetError()
                    servers.append(endpoint)
        return ",".join(servers)

    def set_TLS_auth(self,
                     cert_chain,
                     truststore_path,
                     truststore_pwd,
                     user=None,
                     group=None,
                     mode=None):
        req = json.loads(self.state.request) or {}
        req["cert"] = cert_chain
        self.state.request = json.dumps(req)
        self._set_request()
        super().set_TLS_auth(cert_chain, truststore_path,
                             truststore_pwd, user, group, mode)

    def on_listener_relation_joined(self, event):
        event.relation.data[self.unit]["request"] = self.state.request
        self._get_all_tls_cert()

    def on_listener_relation_changed(self, event):
        self.on_listener_relation_joined(event)
