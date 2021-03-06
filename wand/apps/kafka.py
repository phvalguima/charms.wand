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

"""

Implements the common code across the Kafka stack charms.

There are several common tasks that are contemplated here:
    * install packages: install a list of packages + OpenJDK
    * Authentication: there are several mechanisms and they repeat themselves
    * Render config files such as override.conf for services
    * Manage certificates relations or options

Kafka can come from several distributions and versions. That can be selected
using config options: "distro" and "version". Charms that inherit from this
class should implement these configs.

The Distro allows to select one between 3 options of Kafka packages:

- Confluent:   uses confluent repos to install packages installs the confluent
               stack, including confluent center. Please, check your permissio
               to these packages and appropriate license.
- Apache:      Uses Canonical ESM packages
- Apache Snap: Uses snaps available in snapstore or uploaded as resources.

To use it, Kafka charms must call the equivalent events such as:

class KafkaCharmFinal(KafkaJavaCharmBase):

    def __init__(self, *args):
        ...

    def _on_install(self, event):
        super()._on_install(event)

    def _on_config_changed(self, event):
        super()._on_config_changed(event)

    def _on_update_status(self, event):
        super().on_update_status(event)



Besides the shared event handling as shown above, kafka.py also contains
classes that can be used to setup the LMA integration. For that, the Kafka
final charm should use:

    KafkaJavaCharmBaseNRPEMonitoring
    KafkaJavaCharmBasePrometheusMonitorNode

"""

import time
import base64
import os
import shutil
import subprocess
import logging
import yaml
import socket

import pwd
import grp

from wand.contrib.java import JavaCharmBase
from wand.contrib.linux import (
    userAdd,
    groupAdd,
    LinuxUserAlreadyExistsError,
    LinuxGroupAlreadyExistsError
)

from charmhelpers.core.host import (
    service_running,
)

from ops.framework import (
    Object,
)

from ops.model import (
    BlockedStatus,
    MaintenanceStatus,
    ActiveStatus
)

from charmhelpers.fetch.ubuntu import apt_update
from charmhelpers.fetch.ubuntu import add_source
from charmhelpers.core.host import mount
from charmhelpers.core.templating import render

from wand.security.ssl import setFilePermissions

from wand.apps.relations.tls_certificates import (
    TLSCertificateDataNotFoundInRelationError,
    TLSCertificateRelationNotPresentError
)
from wand.contrib.linux import (
    get_hostname
)
from wand.apps.relations.base_prometheus_monitoring import (
    BasePrometheusMonitor,
    BasePrometheusMonitorMissingEndpointInfoError
)

from nrpe.client import NRPEClient
from charmhelpers.core.hookenv import (
    open_port
)

logger = logging.getLogger(__name__)

# To avoid having to add this template to each charm, generate from a
# string instead.
OVERRIDE_CONF = """{% if service_unit_overrides %}
[Unit]
{% for key, value in service_unit_overrides.items() %}
{% if value %}
{{key}}={{value}}
{% endif %}
{% endfor %}

{% endif %}
[Service]
{% for key, value in service_overrides.items() %}
{% if value %}
{% if key =='ExecStart' %}
# If there is an ExecStart override then we need to clear the ExecStart list first
ExecStart=
{% endif %}
{{key}}={{value}}
{% endif %}
{% endfor %}
{% for key, value in service_environment_overrides.items() %}
{% if value %}
Environment="{{key}}={{value}}"
{% endif %}
{% endfor %}""" # noqa

__all__ = [
    'KafkaJavaCharmBase',
    'KafkaJavaCharmBaseNRPEMonitoring',
    'KafkaJavaCharmBasePrometheusMonitorNode',
    'KafkaCharmBaseConfigNotAcceptedError',
    'KafkaCharmBaseMissingConfigError',
    'KafkaCharmBaseFeatureNotImplementedError'
]


class KafkaJavaCharmBasePrometheusMonitorNode(BasePrometheusMonitor):
    """Prometheus Monitor node issues a request for prometheus2 to
    setup a scrape job against its address.

    Space of the relation will be used to generate address.


    Implement the following methods in the your KafkaJavaCharmBase.

    # Override the method below so the KafkaJavaCharmBase can know if
    # prometheus relation is being used.
    def is_jmxexporter_enabled(self):
        if self.prometheus.relations:
            return True
        return False

    ...

    def __init__(self, *args):

        self.prometheus = \
            KafkaJavaCharmBasePrometheusMonitorNode(
                self, 'prometheus-manual',
                port=self.config.get("jmx-exporter-port", 9404),
                internal_endpoint=self.config.get(
                    "jmx_exporter_use_internal", False),
                labels=self.config.get("jmx_exporter_labels", None))

        # Optionally, listen to prometheus_job_available events
        self.framework.observe(
            self.prometheus.on.prometheus_job_available,
            self.on_prometheus_job_available)
    """

    def __init__(self, charm, relation_name,
                 port=9404, internal_endpoint=False, labels=None):
        """Args:
            charm: CharmBase object
            relation_name: relation name with prometheus
            port: port value to open
            internal_endpoint: defines if the relation
            labels: str comma-separated key=value labels for the job.
                Converted to dict and passed in the request
        """
        super().__init__(charm, relation_name)
        self.port = port
        self.endpoint = \
            self.binding_addr if internal_endpoint else self.advertise_addr
        self.labels = None
        if labels:
            self.labels = {
                la.split("=")[0]: la.split("=")[1] for la in labels.split(",")
            }
        open_port(self.port)
        self.framework.observe(
            self.on.prometheus_job_available,
            self.on_prometheus_job_available)

    def on_prometheus_job_available(self, event):
        try:
            self.scrape_request_all_peers(
                port=self.port,
                metrics_path="/",
                labels=self.labels)
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


class KafkaJavaCharmBaseNRPEMonitoring(Object):
    """Kafka charm used to add NRPE support.

    Create an object of this class to hold the NRPE relation info.


    To instantiate this NRPE, use the following logic:

    def __init__(self, *args):
        self.nrpe = KafkaJavaCharmBaseNRPEMonitoring(
            self,
            svcs=["kafka"],
            endpoints=["127.0.0.1:9000"],
            nrpe_relation_name='nrpe-external-master')


    The class inherits from Object instead of a relation so it can capture
    events for NRPE relation.
    """

    def __init__(self, charm, svcs=[], endpoints=[],
                 nrpe_relation_name='nrpe-external-master'):
        """Intialize with the list of services and ports to be monitored.

        Args:
            svcs: list -> list of service names to be monitored by NRPE
                on systemd.
            endpoints: list -> list of endpoints (IP/hostname:PORT)
                to be monitored by NRPE using check_tcp.
            nrpe_relation_name: str -> name of the relation
        """

        super().__init__(charm, nrpe_relation_name)
        self.nrpe = NRPEClient(charm, nrpe_relation_name)
        self.framework.observe(self.nrpe.on.nrpe_available,
                               self.on_nrpe_available)
        self.services = svcs
        self.endpoints = endpoints
        # uses the StoredState from the parent class
        self.nrpe.state.set_default(is_available=False)

    def recommit_checks(self, svcs, endpoints):
        """Once the relation is built and nrpe-available was already
        executed. Run this method to: (1) clean existing checks; and
        (2) submit new ones. That allows update checks following events
        such as config changes.
        """
        # Update internal values:
        self.svcs = svcs
        self.endpoints = endpoints
        if not self.nrpe.state.is_available:
            # on_nrpe_available not yet called, just update internal
            # values and return
            return
        # Reset checks
        self.nrpe.state.checks = {}
        # Rerun the logic to add checks
        self.on_nrpe_available(None)

    def on_nrpe_available(self, event):
        # Deal with services list first:
        for s in self.services:
            check_name = "check_{}_{}".format(
                self.model.unit.name.replace("/", "_"), s)
            self.nrpe.add_check(command=[
                '/usr/lib/nagios/plugins/check_systemd', s
            ], name=check_name)
        # Deal with endpoints: need to separate the endpoints
        for e in self.endpoints:
            if ":" not in e:
                # No port specified, jump this endpoint
                continue
            hostname = e.split(":")[0]
            port = e.split(":")[1]
            check_name = "check_{}_port_{}".format(
                self.model.unit.name.replace("/", "_"), port)
            self.nrpe.add_check(command=[
                '/usr/lib/nagios/plugins/check_tcp',
                '-H', hostname,
                '-p', port,
            ], name=check_name)
        # Save all new checks to filesystem and to Nagios
        self.nrpe.commit()
        self.nrpe.state.is_available = True


class KafkaCharmBaseMissingRelationError(Exception):

    def __init__(self, relation):
        super().__init__("Missing relation {}".format(relation))
        self._relation = relation

    @property
    def relation(self):
        return self._relation


class KafkaCharmBaseConfigNotAcceptedError(Exception):

    def __init__(self, message):
        super().__init__(message)


class KafkaCharmBaseMissingConfigError(Exception):

    def __init__(self,
                 config_name):
        super().__init__("Missing config: {}".format(config_name))


class KafkaCharmBaseFeatureNotImplementedError(Exception):

    def __init__(self,
                 message="This feature has not"
                         " been implemented yet"):
        super().__init__(message)


class KafkaCharmBaseFailedInstallation(Exception):

    def __init__(self,
                 message="An error occurred during installation: {}",
                 error=""):
        super().__init__(message.format(error))


class KafkaJavaCharmBase(JavaCharmBase):

    LATEST_VERSION_CONFLUENT = "6.1"
    JMX_EXPORTER_JAR_NAME = "jmx_prometheus_javaagent.jar"

    @property
    def unit_folder(self):
        """Returns the unit's charm folder."""

        # Using as a method so we can also mock it on unit tests
        return os.getenv("JUJU_CHARM_DIR")

    @property
    def distro(self):
        """Returns the distro option selected. Values accepted are:

        - Confluent: will use packages coming from confluent repos
        - Apache: uses Canonical's ESM version of kafka
        - Apache Snap: uses snaps instead of packages.

        Children classes should inherit and override this method with
        their own method to load each of the three types above.
        """
        return self.config.get("distro", "confluent").lower()

    def _get_service_name(self):
        """To be overloaded: returns the name of the Kafka service this
        charm must look after.
        """
        return None

    def __init__(self, *args):
        super().__init__(*args)
        self.service = self._get_service_name()
        # This folder needs to be set as root
        os.makedirs("/var/ssl/private", exist_ok=True)
        # Variable to be used to hold TLSCertificatesRelation object
        self.certificates = None
        # List of callable methods that allow to get all the SSL certs/keys
        # This list will be used to iterate over each of the methods on
        # is_ssl_enabled
        self.get_ssl_methods_list = []
        self._kerberos_principal = None
        self.ks.set_default(keytab="")
        self._sasl_protocol = None
        # Save the internal content of keytab file from the action.
        # use it as part of the context in the config_changed
        self.keytab_b64 = ""
        self.services = [self.service]
        self.JMX_EXPORTER_JAR_FOLDER = "/opt/prometheus/"

    def on_update_status(self, event):
        """ This method will update the status of the charm according
            to the app's status

        If the unit is stuck in Maintenance or Blocked, keep the status
        as it is up to the operator to fix these.

        If a child class will implement update-status logic, it must
        call this method as the last task, so the status is correctly set.
        """

        if isinstance(self.model.unit.status, MaintenanceStatus):
            # Log the fact the unit is already blocked and return
            logger.warn(
                "update-status called but unit is in maintenance "
                "status, with message {}, return".format(
                    self.model.unit.status.message))
            return
        if isinstance(self.model.unit.status, BlockedStatus):
            logger.warn(
                "update-status called but unit is blocked, with "
                "message: {}, return".format(
                    self.model.unit.status.message
                )
            )

        # Unit is neither Blocked, nor in Maintenance, start the checks.
        if not self.services:
            self.services = [self.service]

        svc_list = [s for s in self.services if not service_running(s)]
        if len(svc_list) == 0:
            self.model.unit.status = \
                ActiveStatus("{} is running".format(self.service))
            # The status is not in Maintenance and we can see the service
            # is up, therefore we can switch to Active.
            return
        if isinstance(self.model.unit.status, BlockedStatus):
            # Log the fact the unit is already blocked and return
            logger.warn(
                "update-status called but unit is in blocked "
                "status, with message {}, return".format(
                    self.model.unit.status.message))
            return
        self.model.unit.status = \
            BlockedStatus("Services not running that"
                          " should be: {}".format(",".join(svc_list)))

    @property
    def kerberos_principal(self):
        # TODO(pguimaraes): check if domain variable below has len > 0
        # If not, get the IP of the default gateway and rosolve its fqdn
        hostname = "{}.{}".format(
            socket.gethostname(),
            self.config.get("kerberos-domain", ""))

        self._kerberos_principal = "{}/{}@{}".format(
            self.config.get("kerberos-protocol", "").upper(),
            hostname,
            self.config.get("kerberos-realm", "").upper())
        return self._kerberos_principal

    @kerberos_principal.setter
    def kerberos_principal(self, k):
        self._kerberos_principal = k

    @property
    def keytab(self):
        return self.ks.keytab

    @keytab.setter
    def keytab(self, k):
        self.ks.keytab = k

    @property
    def sasl_protocol(self):
        return self._sasl_protocol

    @sasl_protocol.setter
    def sasl_protocol(self, s):
        self._sasl_protocol = s

    def add_certificate_action(self, certs):
        """Adds certificates to self.ks.ssl_certs. This list should be
        used to add custom certificates the entire stack needs to trust.

        Args:
        - certs: multi-line string containing certs in the format:
        ------ BEGIN CERTIFICATE -------
        <data>
        ------ END CERTIFICATE -------
        ------ BEGIN CERTIFICATE .......
        <data>
        ...
        """
        self.ks.ssl_certs.extend([
            "-----BEGIN CERTIFICATE-----\n" + c for c in certs.split(
                "-----BEGIN CERTIFICATE-----\n")])

    def override_certificate_action(self):
        """Empties out all the certs passed via action"""

        self.ks.ss_certs = []

    def _upload_keytab_base64(self, k, filename="kafka.keytab"):
        """Receives the keytab in base64 format and saves to correct file"""

        filepath = "/etc/security/keytabs/{}".format(filename)
        self.set_folders_and_permissions(
            [os.path.dirname(filepath)],
            mode=0o755)
        with open(filepath, "wb") as f:
            f.write(base64.b64decode(k))
            f.close()
        self.keytab_b64 = str(k)
        setFilePermissions(filepath, self.config.get("user", "root"),
                           self.config.get("group", "root"), 0o640)
        self.keytab = filename

    def check_ports_are_open(self, endpoints,
                             retrials=3, backoff=60):
        """Check if a list of ports is open. Should be used with RestartEvent
        processing. That way, one can separate the restart worked or not.

        The idea is to iterate over the port list and check if they are all
        open. If yes, return True. If not, wait for "backoff" seconds and
        retry. If reached "retrials" and still not all the ports are open,
        return False.

        Args:
        - endpoints: list of strings - endpoints in format hostname/IP:PORT
        - retrials: Number of retries that will be executed
        - backoff: Amount of time to wait after a failed trial, in seconds
        """
        for c in range(retrials):
            success = True
            for ep in endpoints:
                sock = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                port = int(ep.split(":")[1])
                ip = ep.split(":")[0]
                if sock.connect_ex((ip, port)) != 0:
                    success = False
                    break
            if success:
                # We were successful in checking each endpoint
                return True
            # Otherwise, we've failed, sleep for some time and retry
            time.sleep(backoff)
        return False

    @property
    def snap(self):
        """Returns the snap name if apache_snap is specified.

        Must be overloaded by each child class to define its own snap name.
        """
        return "kafka"

    def install_packages(self, java_version, packages, snap_connect=None):
        """Install the packages/snaps related to the chosen distro.

        Args:
        java_version: install the openjdk corresponding to that version
        packages: package list to be installed
        snap_connect: interfaces that need to be explicitly connected if snap
                      option is chosen instead.
        """

        MaintenanceStatus("Installing packages")
        version = self.config.get("version", self.LATEST_VERSION_CONFLUENT)
        if self.distro == "confluent":
            url_key = 'https://packages.confluent.io/deb/{}/archive.key'
            key = subprocess.check_output(
                      ['wget', '-qO', '-',
                       url_key.format(version)]).decode("ascii")
            url_apt = \
                'deb [arch=amd64] https://packages.confluent.io/deb/{}' + \
                ' stable main'
            add_source(
                url_apt.format(version),
                key=key)
            apt_update()
        elif self.distro == "apache":
            raise Exception("Not Implemented Yet")
        elif self.distro == "apache_snap":
            # First, try to fetch the snap resource:
            resource = None
            try:
                # Fetch a resource with the same name as the snap.
                # Fetch raises a ModelError if not found
                resource = self.model.resources.fetch(self.snap)
                subprocess.check_output(
                    ["snap", "install", "--dangerous", str(resource)])
            except subprocess.CalledProcessError as e:
                raise KafkaCharmBaseFailedInstallation(
                    error=str(e))
            except Exception:
                # Failed to find a resource, next step is to try install
                # from snapstore
                resource = None
            if not resource:
                # Resource was not found, try install from upstream
                try:
                    subprocess.check_output(
                        ["snap", "install", self.snap,
                         "--channel={}".format(version)])
                except subprocess.CalledProcessError as e:
                    raise KafkaCharmBaseFailedInstallation(
                        error=str(e))
            try:
                if snap_connect:
                    for conn in snap_connect:
                        subprocess.check_output(
                            ["snap", "connect",
                             "{}:{}".format(self.snap, conn)])
            except subprocess.CalledProcessError as e:
                raise KafkaCharmBaseFailedInstallation(
                    error=str(e))
            # Install openjdk for keytool
            super().install_packages(java_version, packages=[])
            # Packages will be already in place for snap
            # no need to run the logic below and setup folders
            return

        # Now, only package type distros are available
        # Setup folders, permissions, etc
        super().install_packages(java_version, packages)
        folders = [
            "/etc/kafka",
            "/var/log/kafka",
            "/var/lib/kafka",
            self.JMX_EXPORTER_JAR_FOLDER]
        self.set_folders_and_permissions(folders)

        # Now, setup jmx exporter logic
        self.jmx_version = self.config.get("jmx_exporter_version", "0.12.0")
        jar_url = self.config.get(
            "jmx_exporter_url",
            "https://repo1.maven.org/maven2/io/prometheus/jmx/"
            "jmx_prometheus_javaagent/{}/"
            "jmx_prometheus_javaagent-{}.jar")
        self.jmx_jar_url = jar_url.format(
                self.jmx_version, self.jmx_version)
        if len(self.jmx_version) == 0 or len(self.jmx_jar_url) == 0:
            # Not enabled, finish the method
            return
        subprocess.check_output(
            ['wget', '-qO',
             self.JMX_EXPORTER_JAR_FOLDER + self.JMX_EXPORTER_JAR_NAME,
             self.jmx_jar_url])
        setFilePermissions(
            self.JMX_EXPORTER_JAR_FOLDER + self.JMX_EXPORTER_JAR_NAME,
            self.config.get("user", "kafka"),
            self.config.get("group", "kafka"), 0o640)

    def _get_api_url(self, advertise_addr):
        """Returns the API endpoint for a given service. If the config
        is not manually set, then the advertise_addr is used instead."""

        return "{}://{}:{}".format(
            "https" if self.is_ssl_enabled() else "http",
            self.config["api_url"] if len(self.config["api_url"]) > 0
            else get_hostname(advertise_addr),
            self.config.get("clientPort", 0)
        )

    def set_folders_and_permissions(self, folders, mode=0o750):
        # Check folder permissions
        MaintenanceStatus("Setting up permissions")
        uid = pwd.getpwnam(self.config.get("user", "root")).pw_uid
        gid = grp.getgrnam(self.config.get("group", "root")).gr_gid
        for f in folders:
            os.makedirs(f, mode=mode, exist_ok=True)
            os.chown(f, uid, gid)

    def is_ssl_enabled(self):
        # We will OR rel_set with each available method for getting cert/key
        # Should start with False
        rel_set = True
        for m in self.get_ssl_methods_list:
            rel_set = rel_set and m()
        if not rel_set:
            logger.warning("Only some of the ssl configurations have been set")
        return rel_set

    def is_rbac_enabled(self):
        if self.distro == "apache":
            return False
        return False

    def _get_ldap_settings(self, mds_urls):
        result = "org.apache.kafka.common.security.oauthbearer." + \
            "OAuthBearerLoginModule required " + \
            'username="{}" password="{}" ' + \
            'metadataServerUrls="{}";'
        return result.format(
            self.config["mds_user"], self.config["mds_password"],
            mds_urls)

    def _cert_relation_set(self, event, rel=None, extra_sans=[]):
        # Will introduce this CN format later
        def __get_cn():
            return "*." + ".".join(socket.getfqdn().split(".")[1:])
        # generate cert request if tls-certificates available
        # rel may be set to None in cases such as config-changed
        # or install events. In these cases, the goal is to run
        # the validation at the end of this method
        if rel:
            if self.certificates.relation:
                sans = [
                    socket.gethostname(),
                    socket.getfqdn()
                ]
                # We do not need to know if any relations exists but rather
                # if binding/advertise addresses exists.
                if rel.binding_addr:
                    sans.append(rel.binding_addr)
                if rel.advertise_addr:
                    sans.append(rel.advertise_addr)
                if rel.hostname:
                    sans.append(rel.hostname)
                sans += extra_sans

                # Common name is always CN as this is the element
                # that organizes the cert order from tls-certificates
                self.certificates.request_server_cert(
                    cn=rel.binding_addr,
                    sans=sans)
            logger.info("Either certificates "
                        "relation not ready or not set")
        # This try/except will raise an exception if tls-certificate
        # is set and there is no certificate available on the relation yet.
        # That will also cause the
        # event to be deferred, waiting for certificates relation to finish
        # If tls-certificates is not set, then the try will run normally,
        # either marking there is no certificate configuration set or
        # concluding the method.
        try:
            if (not self.get_ssl_cert() or not self.get_ssl_key()):
                self.model.unit.status = \
                    BlockedStatus("Waiting for certificates "
                                  "relation or option")
                logger.info("Waiting for certificates relation "
                            "to publish data")
                return False
        # These excepts will treat the case tls-certificates relation is used
        # but the relation is not ready yet
        # KeyError is also a possibility, if get_ssl_cert is called before any
        # event that actually submits a request for a cert is done
        except (TLSCertificateDataNotFoundInRelationError,
                TLSCertificateRelationNotPresentError,
                KeyError):
            self.model.unit.status = \
                BlockedStatus("There is no certificate option or "
                              "relation set, waiting...")
            logger.warning("There is no certificate option or "
                           "relation set, waiting...")
            if event:
                event.defer()
            return False
        return True

    def is_sasl_enabled(self):
        s = self.config.get("sasl-protocol", None)
        if not s:
            return False
        self.sasl_protocol = s.lower()
        s = self.sasl_protocol
        if s == "oauthbearer":
            return self.is_sasl_oauthbearer_enabled()
        if s == "ldap":
            return self.is_sasl_ldap_enabled()
        if s == "scram":
            return self.is_sasl_scram_enabled()
        if s == "plain":
            return self.is_sasl_plain_enabled()
        if s == "delegate-token":
            return self.is_sasl_delegate_token_enabled()
        if s == "kerberos":
            return self.is_sasl_kerberos_enabled()
        if s == "digest":
            return self.is_sasl_digest_enable()
        raise KafkaCharmBaseConfigNotAcceptedError(
            "None of the options for sasl-protocol are accepted. "
            "Please provide one of the following: oauthbearer, scram,"
            "plain, delegate-token, kerberos, digest.")

    def is_sasl_oauthbearer_enabled(self):
        return False

    def is_sasl_scram_enabled(self):
        return False

    def is_sasl_plain_enabled(self):
        return False

    def is_sasl_delegate_token_enabled(self):
        return False

    def is_sasl_ldap_enabled(self):
        if len(self.config.get("mds_user", "")) > 0 and \
           len(self.config.get("mds_password", "")) > 0 and \
           self.distro == "confluent":
            return True
        return False

    def is_sasl_kerberos_enabled(self):
        mandatory_options = [
            "kerberos-protocol",
            "kerberos-realm",
            "kerberos-domain",
            "kerberos-kdc-hostname",
            "kerberos-admin-hostname"
            ]
        c = 0
        for e in mandatory_options:
            if len(self.config.get(e, None)) == 0:
                c += 1
        if c == len(mandatory_options):
            # None of the mandatory items are set, it means
            # the operator does not want it
            return False
        if c == 0:
            # All options are set, we can return True
            return True
        # There are some items unset, warn:
        for e in mandatory_options:
            if len(self.config.get(e, None)) == 0:
                raise KafkaCharmBaseMissingConfigError(e)
        return False

    def is_sasl_digest_enabled(self):
        # TODO(pguimaraes): implement this logic
        return False

    def is_jolokia_enabled(self):
        # TODO(pguimaraes): implement this logic
        return False

    def is_jmxexporter_enabled(self):
        return False

    def _get_confluent_ldap_jaas_config(self,
                                        mds_user, mds_password, mds_urls):
        return 'org.apache.kafka.common.security.oauthbearer.' + \
            'OAuthBearerLoginModule required ' + \
            'username={} password={} '.format(mds_user, mds_password) + \
            'metadataServerUrls="{}"'.format(mds_urls)

    def _on_install(self, event):
        try:
            groupAdd(self.config["group"], system=True)
        except LinuxGroupAlreadyExistsError:
            pass
        try:
            home = "/home/{}".format(self.config["user"])
            userAdd(
                self.config["user"],
                home=home,
                group=self.config["group"])
            os.makedirs(home, 0o755, exist_ok=True)
            shutil.chown(home,
                         user=self.config["user"],
                         group=self.config["group"])
        except LinuxUserAlreadyExistsError:
            pass

    def create_log_dir(self, data_log_dev,
                       data_log_dir,
                       data_log_fs,
                       user="cp-kafka",
                       group="confluent",
                       fs_options=None):

        if len(data_log_dir or "") == 0:
            logger.warning("Data log dir config empty")
            BlockedStatus("data-log-dir missing, please define it")
            return
        os.makedirs(data_log_dir, 0o750, exist_ok=True)
        shutil.chown(data_log_dir,
                     user=self.config["user"],
                     group=self.config["group"])
        dev, fs = None, None
        if len(data_log_dev or "") == 0:
            logger.warning("Data log device not found, using rootfs instead")
        else:
            for k, v in data_log_dev:
                fs = k
                dev = v
            logger.info("Data log device: mkfs -t {}".format(fs))
            cmd = ["mkfs", "-t", fs, dev]
            subprocess.check_call(cmd)
            mount(dev, data_log_dir,
                  options=self.config.get("fs-options", None),
                  persist=True, filesystem=fs)

    def create_data_and_log_dirs(self, data_log_dev,
                                 data_dev,
                                 data_log_dir,
                                 data_dir,
                                 data_log_fs,
                                 data_fs,
                                 user="cp-kafka",
                                 group="confluent",
                                 fs_options=None):

        if len(data_log_dir or "") == 0:
            logger.warning("Data log dir config empty")
            BlockedStatus("data-log-dir missing, please define it")
            return
        if len(data_dir or "") == 0:
            logger.warning("Data dir config empty")
            BlockedStatus("data-dir missing, please define it")
            return
        os.makedirs(data_log_dir, 0o750, exist_ok=True)
        shutil.chown(data_log_dir,
                     user=self.config["user"],
                     group=self.config["group"])
        os.makedirs(data_dir, 0o750, exist_ok=True)
        shutil.chown(data_dir,
                     user=self.config["user"],
                     group=self.config["group"])
        dev, fs = None, None
        if len(data_log_dev or "") == 0:
            logger.warning("Data log device not found, using rootfs instead")
        else:
            for k, v in data_log_dev.items():
                fs = k
                dev = v
            logger.info("Data log device: mkfs -t {}".format(fs))
            cmd = ["mkfs", "-t", fs, dev]
            subprocess.check_call(cmd)
            mount(dev, data_log_dir,
                  options=self.config.get("fs-options", None),
                  persist=True, filesystem=fs)

        if len(data_dev or "") == 0:
            logger.warning("Data device not found, using rootfs instead")
        else:
            for k, v in data_dev.items():
                fs = k
                dev = v
            logger.info("Data log device: mkfs -t {}".format(fs))
            cmd = ["mkfs", "-t", fs, dev]
            subprocess.check_call(cmd)
            mount(dev, data_dir,
                  options=self.config.get("fs-options", None),
                  persist=True, filesystem=fs)

    # To be used on parameter: confluent.license.topic
    def get_license_topic(self):
        if self.distro == "confluent":
            # If unset, return it empty
            return self.config.get("confluent_license_topic")
        return None

    def render_service_override_file(
            self, target,
            jmx_jar_folder="/opt/prometheus/",
            jmx_file_name="/opt/prometheus/prometheus.yaml",
            extra_envvars=None):
        """Renders the service override.conf file.

        In the snap deployment, jmx will be placed on a /snap and
        prometheus.yaml will be placed on /var/snap.

        target: filepath for the override file
        jmx_jar_folder: either /opt on confluent or /snap on apache_snap
        jmx_file_name: either /opt on confluent or /var/snap for snap
        extra_envvars: allows a charm to specify if any additional env
                       vars should be added. This value will take priority
                       over the config options and any configs set by this
                       class.
        """

        service_unit_overrides = yaml.safe_load(
            self.config.get('service-unit-overrides', ""))
        service_overrides = yaml.safe_load(
            self.config.get('service-overrides', ""))
        service_environment_overrides = yaml.safe_load(
            self.config.get('service-environment-overrides', ""))

        if "KAFKA_OPTS" not in service_environment_overrides:
            # Assume it will be needed, so adding it
            service_environment_overrides["KAFKA_OPTS"] = ""
        kafka_opts = []
        if self.is_ssl_enabled():
            kafka_opts.append("-Djdk.tls.ephemeralDHKeySize=2048")
        if self.is_sasl_enabled():
            kafka_opts.append(
                "-Djava.security.auth.login.config="
                "/etc/kafka/jaas.conf")
        if self.is_jolokia_enabled():
            kafka_opts.append(
                "-javaagent:/opt/jolokia/jolokia.jar="
                "config=/etc/kafka/jolokia.properties")
        if self.is_jmxexporter_enabled():
            kafka_opts.append(
                "-javaagent:{}={}:{}"
                .format(
                    jmx_jar_folder + self.JMX_EXPORTER_JAR_NAME,
                    self.config.get("jmx-exporter-port", 9404),
                    jmx_file_name))
            render(source="prometheus.yaml",
                   target=jmx_file_name,
                   owner=self.config.get('user'),
                   group=self.config.get("group"),
                   perms=0o644,
                   context={})
        if len(kafka_opts) == 0:
            # Assumed KAFKA_OPTS would be set at some point
            # however, it was not, so removing it
            service_environment_overrides.pop("KAFKA_OPTS", None)
        else:
            service_environment_overrides["KAFKA_OPTS"] = \
                '{}'.format(" ".join(kafka_opts))
            # Now, ensure all the kafka charms have their OPTS set:
            if "SCHEMA_REGISTRY_OPTS" not in service_environment_overrides:
                service_environment_overrides["SCHEMA_REGISTRY_OPTS"] = \
                    service_environment_overrides["KAFKA_OPTS"]
            if "KSQL_OPTS" not in service_environment_overrides:
                service_environment_overrides["KSQL_OPTS"] = \
                    service_environment_overrides["KAFKA_OPTS"]
            if "KAFKAREST_OPTS" not in service_environment_overrides:
                service_environment_overrides["KAFKAREST_OPTS"] = \
                    service_environment_overrides["KAFKA_OPTS"]
            if "CONTROL_CENTER_OPTS" not in service_environment_overrides:
                service_environment_overrides["CONTROL_CENTER_OPTS"] = \
                    service_environment_overrides["KAFKA_OPTS"]
        if extra_envvars:
            for k, v in extra_envvars.items():
                service_environment_overrides[k] = v

        # Even if service_overrides is not defined, User and Group need to be
        # correctly set if this option was passed to the charm.
        if not service_overrides:
            service_overrides = {}
        for d in ["User", "Group"]:
            dlower = d.lower()
            if dlower in self.config and \
               len(self.config.get(dlower, "")) > 0:
                service_overrides[d] = self.config.get(dlower)
        self.set_folders_and_permissions([os.path.dirname(target)])
        render(source="kafka_override.conf.j2",
               target=target,
               owner=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o644,
               context={
                   "service_unit_overrides": service_unit_overrides or {},
                   "service_overrides": service_overrides or {},
                   "service_environment_overrides": service_environment_overrides or {} # noqa
               })
        # Shortening the name
        svc_override = service_environment_overrides
        return {
            "service_unit_overrides": service_unit_overrides or {},
            "service_overrides": service_overrides or {},
            "service_environment_overrides": svc_override or {},
            "is_jmx_exporter_enabled": self.is_jmxexporter_enabled()
        }

    def _render_jaas_conf(self, jaas_path="/etc/kafka/jaas.conf"):
        content = ""
        if self.is_sasl_kerberos_enabled():
            krb = """Server {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="/etc/security/keytabs/{}"
    storeKey=true
    useTicketCache=false
    principal="{}";
}};
""".format(self.keytab, self.kerberos_principal) # noqa
            content += krb
        self.set_folders_and_permissions([os.path.dirname(jaas_path)])
        with open(jaas_path, "w") as f:
            f.write(content)
        setFilePermissions(jaas_path, self.config.get("user", "root"),
                           self.config.get("group", "root"), 0o640)
        return content

    def _render_krb5_conf(self):
        enctypes = "aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96" + \
            " arc-four-hmac rc4-hmac"
        ctx = {
                "realm": self.config["kerberos-realm"],
                "dns_lookup_realm": "false",
                "dns_lookup_kdc": "false",
                "ticket_lifetime": "24h",
                "forwardable": "true",
                "udp_preference_limit": "1",
                "default_tkt_enctypes": enctypes,
                "default_tgs_enctypes": enctypes,
                "permitted_enctypes": enctypes,
                "kdc_hostname": self.config["kerberos-kdc-hostname"],
                "admin_hostname": self.config["kerberos-admin-hostname"]
            }
        render(source="krb5.conf.j2",
               target="/etc/krb5.conf",
               owner=self.config.get("user", "root"),
               group=self.config.get("group", "root"),
               perms=0o640,
               context=ctx)
        return ctx

    def _on_config_changed(self, event):
        """Implements the JAAS logic. Returns changes in config files.
        """

        changed = {}
        if self.is_sasl_kerberos_enabled():
            changed["krb5_conf"] = self._render_krb5_conf()
            changed["jaas"] = self._render_jaas_conf()
        return changed
