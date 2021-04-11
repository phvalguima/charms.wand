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

import os
import shutil
import yaml
import subprocess
import logging

from wand.contrib.java import JavaCharmBase

from ops.model import BlockedStatus

from charmhelpers.fetch.ubuntu import apt_update
from charmhelpers.fetch.ubuntu import add_source
from charmhelpers.fetch.ubuntu import apt_install
from charmhelpers.core.host import mount

logger = logging.getLogger(__name__)


class KafkaJavaCharmBase(JavaCharmBase):

    LATEST_VERSION_CONFLUENT = "6.1"

    @property
    def distro(self):
        return self.options.get("distro", "confluent").lower()

    def __init__(self, *args):
        super().__init__(*args)

    def install_packages(self, java_version, packages):
        version = self.config.get("version", self.LATEST_VERSION_CONFLUENT)
        if self.distro == "confluent":
            url_key = 'https://packages.confluent.io/deb/{}/archive.key'
            key = subprocess.check_output(['wget', '-qO', '-',
                                           url_key.format(version)])
            url_apt = \
                'deb [arch=amd64] https://packages.confluent.io/deb/{}' + \
                ' stable main'
            add_source(
                url_apt.format(version),
                key=key)
            apt_update()
        elif self.distro == "apache":
            raise Exception("Not Implemented Yet")

        apt_install(packages)

    def is_ssl_enabled(self):
        if len(self.config.get("ssl_ca", "")) > 0 and \
           len(self.config.get("ssl_cert", "")) > 0 and \
           len(self.config.get("ssl_key", "")) > 0:
            return True
        if len(self.config.get("ssl_ca", "")) > 0 or \
           len(self.config.get("ssl_cert", "")) > 0 or \
           len(self.config.get("ssl_key", "")) > 0:
            logger.warning("Only some of the ssl configurations have been set")
        return False

    def is_sasl_kerberos_enabled(self):
        # TODO(pguimaraes): implement this logic
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
        os.mkdir(data_log_dir, 0o750)
        shutil.chown(data_log_dir,
                     user=self.config["zookeeper-user"],
                     group=self.config["zookeeper-group"])
        os.mkdir(data_dir, 0o750)
        shutil.chown(data_dir,
                     user=self.config["zookeeper-user"],
                     group=self.config["zookeeper-group"])
        dev, fs = None, None
        if len(data_log_dev or "") == 0:
            logger.warning("Data log device not found, using rootfs instead")
        else:
            for k, v in yaml.safe_load(data_log_dev):
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
            for k, v in yaml.safe_load(data_dev):
                fs = k
                dev = v
            logger.info("Data log device: mkfs -t {}".format(fs))
            cmd = ["mkfs", "-t", fs, dev]
            subprocess.check_call(cmd)
            mount(dev, data_dir,
                  options=self.config.get("fs-options", None),
                  persist=True, filesystem=fs)
