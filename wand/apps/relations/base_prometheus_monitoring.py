"""

Implements the provider side Prometheus Manual relation for the
Operator Framework.

Submits a job request to prometheus with the following format:

{
    "job_name": ...
    "job_data": {
        "static_configs":[{
            "targets": # list of targets
            labels: # dict of key: value for custom labels
        }, {}...],
    },
    "scheme": "http" or "https",
    "metrics_path": # HTTP path to the metrics endpoint,

    # If scheme is set to "https", then specify a custom ca_file
    # The actual ca value is passed as an extra config on the send call
    "tls_config": {'ca_file': '__ca_file__'}
}

There are two types of manual jobs that can be submitted:
1) per-unit scrape: run BasePrometheusMonitor.scrape_request
2) per-application scrape: run BasePrometheusMonitor.scrape_request_all_peers

The former will create a scrape job in prometheus for each of the units
present in the relation. The latter will be executed only by the leader unit.
It will first search for the peers relation name in the metadata.yaml. Once it
is found, it will get all the endpoints for each of the peers.

Why do we need a per-application job?
In some cases, the prometheus data needs to data from several targets under
the same job. One example is Kafka's grafana dashboards.

IMPORTANT:
A per-application is only possible if 'peers' relation is defined in the
metadata.yaml. If there is no peers relation and scrap_request_all_peers is
called, then raise an Exception.


HOW TO USE:

class MyCharm(CharmBase):

    def __init__(self, *args):
        self.prometheus = BasePrometheusMonitor(self, 'prometheus-manual')
        self.framework.observe(
            self.nrpe_client.on.prometheus_job_available,
            self.on_prometheus_job_available)

    def on_prometheus_job_available(self, event):
        # Now process a per-unit or per-application case:
        # self.scrape_request(...) or self.scrape_request_all_peers(...)
        .....



    def on_prometheus_job_available(self, event):
        try:
            self.scrape_request_all_peers(.....)

        except BasePrometheusMonitorMissingEndpointInfoError:
            # This is possible to happen if the following sequence
            # of events happens:
            # 1) cluster-changed: new peer updated and added endpoint
            # 2) prometheus-changed event: peer info recovered
            #       issue the -available event
            # 3) cluster-joined: new peer in the relation
            # 4) prometheus-available:
            #       there was no time for the new peer to add its own
            #       endpoint information. Reinvoke prometheus-changed
            #       on the worst case, this event will be deferred
            self.on_prometheus_relation_changed(event)



    def on_peer_relation_changed(self, event):
        # If peer relation is needed (i.e., using scrape_request_all_peers)
        # then we need to rerun self.prometheus.on_prometheus_relation_changed
        # That will ensure either the peer-changed updates with a complete
        # request or is deferred until the new units have published their own
        # prometheus endpoints
        if len(self.framework.model.relations['prometheus-manual']) > 0:
            # We know there is a prometheus relation, then rerun:
            self.prometheus.on_prometheus_relation_changed(event)
        .....
"""

import uuid
from ops.framework import EventBase, EventSource, StoredState
from ops.charm import CharmEvents

from charmhelpers.core import hookenv

from wand.apps.relations.relation_manager_base import RelationManagerBase


def _implicit_peer_relation_name():
    md = hookenv.metadata()
    if 'peers' in md:
        return sorted(md['peers'].keys())[0]
    return None


class BasePrometheusMonitorNoPeerRelationFoundError(Exception):
    def __init__(
        self,
        msg="No peers relation available, scrape_request_all_peers "
            "should not be used."):
        super().__init__(msg)


class BasePrometheusMonitorMissingEndpointInfoError(Exception):
    def __init__(self,
                 msg="Missing one endpoint info, defer event advised."):
        super().__init__(msg)


class BasePrometheusMonitorAvailable(EventBase):
    pass


class BasePrometheusMonitorAvailableEvents(CharmEvents):
    prometheus_job_available = EventSource(BasePrometheusMonitorAvailable)


class BasePrometheusMonitor(RelationManagerBase):

    on = BasePrometheusMonitorAvailableEvents()
    state = StoredState()

    def __init__(self, charm, relation_name, endpoint=None):
        super().__init__(charm, relation_name)
        self.endpoint = endpoint
        self.state.set_default(peer_rel_name="")
        self.framework.observe(
            charm.on[relation_name].relation_joined,
            self.on_prometheus_relation_joined)
        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self.on_prometheus_relation_changed)

    @property
    def peer_rel_name(self):
        if len(self.state.peer_rel_name) == 0:
            return None
        return self.state.peer_rel_name

    @peer_rel_name.setter
    def peer_rel_name(self, r):
        self.state.peer_rel_name = r

    def scrape_request_all_peers(self, port, metrics_path,
                                 ca_cert=None, job_name=None,
                                 labels=None):
        """Request prometheus job for the entire application.

        If this is not a leader unit, return. Also, if peer relation does not
        exist, then raise an Exception.

        Search each peer for the data in self.relation_name + "_endpoint"
        Once that is found, generate a list of targets. All the options in
        this method are equal to the self.scrape_request otherwise.
        """
        if not self._charm.unit.is_leader():
            return
        self.peer_rel_name = _implicit_peer_relation_name() or ""
        if not self.peer_rel_name:
            raise BasePrometheusMonitorNoPeerRelationFoundError()

        targets = [
            "{}:{}".format(self.endpoint or self.advertise_addr, port)
        ]
        peer_rel = self.framework.model.relations[self.peer_rel_name][0]
        for u in peer_rel.units:
            entryname = self._relation_name + "_endpoint"
            if entryname not in peer_rel.data[u]:
                raise BasePrometheusMonitorMissingEndpointInfoError()
            targets.append(
                "{}:{}".format(
                    peer_rel.data[u][entryname], port))

        name = job_name or \
            "{}".format(self._charm.app.name.replace("-", "_"))
        data = {
            'job_name': name,
            'job_data': {
                'static_configs': [{
                    'targets': targets
                }],
                'scheme': 'http',
                'metrics_path': metrics_path
            }
        }
        lbs = labels or {}
        # Those two labels are needed always.
        # In case job is unset, prometheus will override it with:
        # job_name-<UUID>
        # Which most of upstream grafana dashboards will not work well.
        if "job" not in lbs:
            lbs["job"] = name
        if "env" not in lbs:
            lbs["env"] = "juju"
        for c in range(0, len(data["job_data"]["static_configs"])):
            data["job_data"]["static_configs"][c]["labels"] = lbs
        if ca_cert:
            data['tls_config'] = {'ca_file': '__ca_file__'}
            data['scheme'] = 'https'
            self.request(name, ca_cert=ca_cert, job_data=data)
            return
        self.request(name, job_data=data)

    def scrape_request(self,
                       port,
                       metrics_path,
                       endpoint,
                       ca_cert=None,
                       job_name=None,
                       labels=None):
        """Request registers the Prometheus scrape job.
        port: to be used as part of the target

        Args:
        - port: port to be targeted by prometheus for scrape
        - metrics_path: HTTP request path
        - endpoint: target endpoint
        - ca_cert: certificate to get data, may or may not be used
        - job_name: it is possible to specify custom name
        - labels: dict containing key=value labels to be attached
                  to this source
        """
        # advertise_addr given that minio endpoint uses advertise_addr
        # to find its hostname
        name = job_name or \
            "{}_node".format(self._charm.unit.name.replace("/", "-"))
        data = {
            'job_name': name,
            'job_data': {
                'static_configs': [{
                    'targets': ["{}:{}".format(
                        endpoint or self.advertise_addr, port)]
                }],
                'scheme': 'http',
                'metrics_path': metrics_path
            }
        }
        lbs = labels or {}
        # Those two labels are needed always.
        # In case job is unset, prometheus will override it with:
        # job_name-<UUID>
        # Which most of upstream grafana dashboards will not work well.
        if "job" not in lbs:
            lbs["job"] = name
        if "env" not in lbs:
            lbs["env"] = "juju"
        for c in range(0, len(data["job_data"]["static_configs"])):
            data["job_data"]["static_configs"][c]["labels"] = lbs
        if ca_cert:
            data['tls_config'] = {'ca_file': '__ca_file__'}
            data['scheme'] = 'https'
            self.request(name, ca_cert=ca_cert, job_data=data)
            return
        self.request(name, job_data=data)

    # Keeping it for compatibility with other charms
    def request(self, job_name, ca_cert=None, job_data=None):
        # Job name as field and value the json describing it
        req_uuid = str(uuid.uuid4())
        job_data["request_id"] = req_uuid
        self.send("request_" + req_uuid, job_data)

    def on_prometheus_relation_joined(self, event):
        """Set the advertise address of each unit in the peer relation.
        Binding addresses do not change on unit lifecycle, so it is safe to
        run this logic once.

        If no peer relation exists, then ignore this task.
        """
        self.peer_rel_name = _implicit_peer_relation_name()
        if not self.peer_rel_name:
            return
        peer_rel = self.framework.model.relations[self.peer_rel_name][0]
        peer_rel.data[self.unit][self._relation_name + "_endpoint"] = \
            self.endpoint or \
            self.model.get_binding(self.peer_rel_name).network.ingress_address

    def on_prometheus_relation_changed(self, event):
        """If peers relation exist, ensure all the peers published their
        endpoints before moving on. If the info is set, then emit a
        BasePrometheusMonitorAvailable.
        """
        if not self.peer_rel_name:
            # There is no cluster relation, just emit the event
            self.on.prometheus_job_available.emit()
            return

        peer_rel = self.framework.model.relations[self.peer_rel_name][0]
        # Check if all units have already published their endpoints.
        # If one did not yet, then defer this event and return.
        entryname = self._relation_name + "_endpoint"
        if entryname not in peer_rel.data[self.unit]:
            # A -changed event is not expected to happen before a -joined.
            # Therefore this logic will likely never be used.
            event.defer()
            return

        for u in peer_rel.units:
            if entryname not in peer_rel.data[u]:
                # Prometheus does not yet has all the info it needs.
                # Postpone this event:
                event.defer()
                return

        self.on.prometheus_job_available.emit()
