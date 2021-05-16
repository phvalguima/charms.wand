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

import base64
import os
import shutil
import subprocess
import logging
import yaml
from socket import gethostname

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

logger = logging.getLogger(__name__)

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
    'KafkaCharmBaseConfigNotAcceptedError',
    'KafkaCharmBaseMissingConfigError',
    'KafkaCharmBaseFeatureNotImplementedError'
]


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


class KafkaJavaCharmBase(JavaCharmBase):

    LATEST_VERSION_CONFLUENT = "6.1"

    @property
    def unit_folder(self):
        # Using as a method so we can also mock it on unit tests
        return os.getenv("JUJU_CHARM_DIR")

    @property
    def distro(self):
        return self.config.get("distro", "confluent").lower()

    def _get_service_name(self):
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

    def on_update_status(self, event):
        if not service_running(self.service) and \
           not isinstance(self.model.unit.status, ActiveStatus):
            return
        if not service_running(self.service):
            self.model.unit.status = \
                BlockedStatus("{} not running".format(self.service))
            return
        self.model.unit.status = \
            ActiveStatus("{} is running".format(self.service))

    @property
    def kerberos_principal(self):
        # TODO(pguimaraes): check if domain variable below has len > 0
        # If not, get the IP of the default gateway and rosolve its fqdn
        hostname = "{}.{}".format(
            gethostname(),
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

    def _upload_keytab_base64(self, k, filename="kafka.keytab"):
        """Receives the keytab in base64 format and saves to correct file"""
        filepath = "/etc/security/keytabs/{}".format(filename)
        self.set_folders_and_permissions(
            [os.path.dirname(filepath)],
            mode=0o755)
        with open(filepath, "wb") as f:
            f.write(base64.b64decode(k))
            f.close()
        setFilePermissions(filepath, self.config.get("user", "root"),
                           self.config.get("group", "root"), 0o640)
        self.keytab = filename

    def install_packages(self, java_version, packages):
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
        super().install_packages(java_version, packages)
        folders = ["/etc/kafka", "/var/log/kafka", "/var/lib/kafka"]
        self.set_folders_and_permissions(folders)

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

    def _cert_relation_set(self, event, rel=None, extra_sans=[]):
        # generate cert request if tls-certificates available
        # rel may be set to None in cases such
        # as config-changed or install events
        # In these cases, the goal is to run the
        # validation at the end of this method
        if rel:
            if self.certificates.relation and rel.relation:
                sans = [
                    rel.binding_addr,
                    rel.advertise_addr,
                    rel.hostname,
                    gethostname()
                ]
                sans += extra_sans
                # Common name is always CN as this is the element
                # that organizes the cert order from tls-certificates
                self.certificates.request_server_cert(
                    cn=rel.binding_addr,
                    sans=sans)
            logger.info("Either certificates "
                        "relation not ready or not set")
        # This try/except will raise an exception if
        # tls-certificate is set and there is no
        # certificate available on the relation yet. That will also cause the
        # event to be deferred, waiting for certificates relation to finish
        # If tls-certificates is not set, then the try
        # will run normally, either
        # marking there is no certificate configuration
        # set or concluding the method.
        try:
            # Iterated over each of the methods present on the list and all  of
            # them returned a cert or key.
            if not self.is_ssl_enabled():
                self.model.unit.status = \
                    BlockedStatus("Waiting for certificates"
                                  " relation or option")
                logger.info("Waiting for certificates"
                            " relation to publish data")
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
        # TODO(pguimaraes): implement this logic
        return False

    def _on_install(self, event):
        try:
            groupAdd(self.config["group"], system=True)
        except LinuxGroupAlreadyExistsError:
            pass
        try:
            userAdd(self.config["user"], group=self.config["group"])
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

    def render_service_override_file(self,
                                     target,
                                     jmx_file_name="kafka"):
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
                "-javaagent:/opt/prometheus/jmx_prometheus_javaagent.jar="
                "{}:/opt/prometheus/{}.yml"
                .format(self.config.get("jmx-exporter-port", 8079),
                        jmx_file_name))
        if len(kafka_opts) == 0:
            # Assumed KAFKA_OPTS would be set at some point
            # however, it was not, so removing it
            service_environment_overrides.pop("KAFKA_OPTS", None)
        else:
            service_environment_overrides["KAFKA_OPTS"] = \
                '{}'.format(" ".join(kafka_opts))

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
        changed = {}
        if self.is_sasl_kerberos_enabled():
            changed["krb5_conf"] = self._render_krb5_conf()
            changed["jaas"] = self._render_jaas_conf()
        return changed
