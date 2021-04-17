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

import unittest
import logging
import os
import shutil
from mock import patch

from ops.testing import Harness

import wand.apps.kafka as kafka
import wand.contrib.java as java
from wand.contrib.linux import getCurrentUserAndGroup


# Set logger to the module to be mocked
logger = logging.getLogger("wand.apps.kafka")

OVERRIDE_CONF = """
[Service]

User=test

Group=test

Environment=\"KAFKA_HEAP_OPTS=-Xmx1g\"
Environment=\"KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:/etc/kafka/zookeeper-log4j.properties\"
Environment=\"LOG_DIR=/var/log/kafka\"
Environment=\"KAFKA_OPTS=-Djdk.tls.ephemeralDHKeySize=2048 -Djava.security.auth.login.config=/etc/kafka/jaas.conf\"
"""  # noqa

SVC_ENV_OVERRIDE = """KAFKA_HEAP_OPTS: '-Xmx1g'
KAFKA_LOG4J_OPTS: '-Dlog4j.configuration=file:/etc/kafka/zookeeper-log4j.properties'
LOG_DIR: '/var/log/kafka'""" # noqa


class TestAppKafka(unittest.TestCase):
    maxDiff = None

    def _simulate_render(self, ctx=None, templ_file=""):
        import jinja2
        env = jinja2.Environment(loader=jinja2.FileSystemLoader('templates'))
        templ = env.get_template(templ_file)
        doc = templ.render(ctx)
        return doc

    def setUp(self):
        super(TestAppKafka, self).setUp()
        os.environ["JUJU_CHARM_DIR"] = "./"

    @patch.object(logger, "warning")
    @patch.object(shutil, "chown")
    @patch.object(os, "makedirs")
    def test_create_log_dir(self,
                            mock_mkdir,
                            mock_chown,
                            mock_warning):
        harness = Harness(kafka.KafkaJavaCharmBase)
        self.addCleanup(harness.cleanup)
        harness.begin()
        k = harness.charm
        # Ensure we can skip if data_log_dev is empty
        k.create_log_dir(data_log_dev=None,
                         data_log_dir="",
                         data_log_fs=None)
        mock_warning.assert_called()

    @patch.object(kafka.KafkaJavaCharmBase, "set_folders_and_permissions")
    @patch.object(java.JavaCharmBase, "install_packages")
    @patch.object(kafka, "apt_update")
    @patch.object(kafka, "add_source")
    def test_install_packages(self,
                              mock_add_source,
                              mock_apt_update,
                              mock_java_inst_pkgs,
                              mock_set_folders_perms):
        harness = Harness(kafka.KafkaJavaCharmBase)
        self.addCleanup(harness.cleanup)
        harness.begin()
        harness._update_config({
            "distro": "confluent",
            "version": "6.1"
        })
        k = harness.charm
        k.install_packages("openjdk-11-headless", ["test"])
        self.assertIn(
            "deb [arch=amd64] https://packages.confluent.io/deb/6.1" +
            " stable main", mock_add_source.call_args[0])
        mock_apt_update.assert_called()
        mock_java_inst_pkgs.assert_called()

    @patch.object(logger, "warning")
    @patch.object(shutil, "chown")
    @patch.object(os, "makedirs")
    def test_create_data_and_log_dirs(self,
                                      mock_mkdir,
                                      mock_chown,
                                      mock_warning):
        harness = Harness(kafka.KafkaJavaCharmBase)
        self.addCleanup(harness.cleanup)
        harness.begin()
        k = harness.charm
        k.create_data_and_log_dirs(
            data_log_dev=None,
            data_dev="",
            data_log_dir="",
            data_dir=None,
            data_log_fs="",
            data_fs=None)
        mock_warning.assert_called()

    @patch.object(kafka.KafkaJavaCharmBase, "set_folders_and_permissions")
    @patch.object(kafka, "render")
    @patch.object(kafka.KafkaJavaCharmBase, "is_sasl_kerberos_enabled")
    @patch.object(kafka.KafkaJavaCharmBase, "is_ssl_enabled")
    def test_render_override_conf(self,
                                  mock_ssl_enabled,
                                  mock_krbs,
                                  mock_render,
                                  mock_set_folder_perms):
        def __cleanup():
            try:
                os.remove("/tmp/13fnutest/13fnutest.service")
                os.remove("/tmp/13fnutest")
            except: # noqa
                pass

        __cleanup()
        mock_render.return_value = ""
        mock_ssl_enabled.return_value = True
        mock_krbs.return_value = True
        harness = Harness(kafka.KafkaJavaCharmBase)
        self.addCleanup(harness.cleanup)
        harness.begin()
        k = harness.charm
        k.service = "13fnutest"
        # We do not have config-changed here
        # and we do not want to trigger it
        user, group = getCurrentUserAndGroup()
        harness._update_config(key_values={
            "user": "test",
            "group": "test",
            "service-unit-overrides": '',
            "service-overrides": "",
            "service-environment-overrides": SVC_ENV_OVERRIDE,
        })
        k.render_service_override_file(
            target="/tmp/13fnutest/13fnutest.service")
        mock_render.assert_called()
        rendered = self._simulate_render(
            ctx=mock_render.call_args.kwargs["context"],
            templ_file="kafka_override.conf.j2")
        self.assertEqual(OVERRIDE_CONF, rendered)
        __cleanup()
