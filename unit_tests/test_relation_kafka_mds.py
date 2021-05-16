import unittest
from mock import (
    patch,
    PropertyMock
)

import wand.apps.relations.kafka_mds as mds


# Helper classes to allow mocking of some
# operator-specific objects
class model:
    def __init__(self, app):
        self._app = app

    @property
    def app(self):
        return self._app


class relation:
    def __init__(self, data):
        self._data = data

    @property
    def data(self):
        return self._data


class event:
    def __init__(self, relation):
        self._relation = relation

    @property
    def relation(self):
        return self._relation


class unit:
    def __init__(self, is_leader):
        self._is_leader = is_leader

    def is_leader(self):
        return self._is_leader


class TestKafkaSchemaRegistryMDSRequiresRelation(unittest.TestCase):
    maxDiff = None

    APP_NAME = "kafka_sr"
    MODEL = model(APP_NAME)

    def setUp(self):
        super(TestKafkaSchemaRegistryMDSRequiresRelation, self).setUp()

    @patch.object(mds.KafkaSchemaRegistryMDSRequiresRelation,
                  "_get_cluster_id_via_mds")
    # Need to replace __init__ because we do not want to go through
    # the entire logic of Harness and operator framework here
    @patch.object(mds.KafkaSchemaRegistryMDSRequiresRelation, "__init__")
    @patch.object(mds.KafkaSchemaRegistryMDSRequiresRelation,
                  "unit", new_callable=PropertyMock)
    # Mock RBAC given this test will assume it is all correctly set
    @patch.object(mds.KafkaSchemaRegistryMDSRequiresRelation,
                  "_check_rbac_enabled")
    @patch.object(mds.KafkaSchemaRegistryMDSRequiresRelation,
                  "super_user_list", new_callable=PropertyMock)
    @patch.object(mds.KafkaSchemaRegistryMDSRequiresRelation,
                  "req_params", new_callable=PropertyMock)
    @patch.object(mds.KafkaSchemaRegistryMDSRequiresRelation,
                  "model", new_callable=PropertyMock)
    @patch.object(mds.KafkaSchemaRegistryMDSRequiresRelation,
                  "_post_request")
    def test_on_mds_relation_changed(self,
                                     mock_post_req,
                                     mock_model,
                                     mock_req_params,
                                     mock_super_users,
                                     mock_check_rbac,
                                     mock_unit,
                                     mock_init,
                                     mock_get_cluster_id):
        # __init__ must return None
        mock_init.return_value = None
        mock_unit.return_value = unit(True)
        mock_super_users.return_value = ["a", "b"]
        mock_get_cluster_id.return_value = "1234"
        mock_req_params.return_value = {
            "kafka_sr_cluster_name": "sr-test",
            "kafka-cluster-id": "1234",
            "group-id": "43",
            "kafka-hosts": {
                "host": "test1.example.org",
                "port": 123
            },
            "kafkastore-topic": "kafkastore-topic",
            "rest-advertised-protocol": "rest-advertised-protocol",
            "confluent-license-topic": "confluent-license-topic",
            "kafka_sr_ldap_user": "sr"
        }
        mock_model.return_value = self.MODEL
        rel = relation({
            self.APP_NAME: {
                "rbac_request_sent": "false"
            }
        })
        obj = mds.KafkaSchemaRegistryMDSRequiresRelation(None, "kafka_sr")
        obj.on_mds_relation_changed(event(rel))
        mock_post_req.assert_any_call('/security/1.0/principals/User:a/roles/SystemAdmin', {'clusters': {'kafka-cluster': '1234', 'connect-cluster': '43'}}) # noqa
        mock_post_req.assert_any_call('/security/1.0/principals/User:b/roles/SystemAdmin', {'clusters': {'kafka-cluster': '1234', 'connect-cluster': '43'}}) # noqa
        mock_post_req.assert_any_call('/security/1.0/registry/clusters', [{'clusterName': 'sr-test', 'scope': {'clusters': {'kafka-cluster': '1234', 'schema-registry-cluster': '43'}}, 'hosts': {'host': 'test1.example.org', 'port': 123}, 'protocol': 'rest-advertised-protocol'}]) # noqa
        mock_post_req.assert_any_call('/security/1.0/principals/User:sr/roles/SecurityAdmin', {'clusters': {'kafka-cluster': '1234', 'schema-registry-cluster': '43'}}) # noqa
        mock_post_req.assert_any_call('/security/1.0/principals/User:sr/roles/ResourceOwner/bindings', {'scope': {'clusters': {'kafka-cluster': '1234'}, 'resourcePatterns': [{'resourceType': 'Group', 'name': '43', 'patternType': 'LITERAL'}, {'resourceType': 'Topic', 'name': 'kafkastore-topic', 'patternType': 'LITERAL'}, {'resourceType': 'Topic', 'name': 'confluent-license-topic', 'patternType': 'LITERAL'}]}}) # noqa


class TestKafkaConnectMDSRequiresRelation(unittest.TestCase):
    maxDiff = None

    APP_NAME = "kafka_connect"
    MODEL = model(APP_NAME)

    def setUp(self):
        super(TestKafkaConnectMDSRequiresRelation, self).setUp()

    @patch.object(mds.KafkaConnectMDSRequiresRelation,
                  "_get_cluster_id_via_mds")
    # Need to replace __init__ because we do not want to go through
    # the entire logic of Harness and operator framework here
    @patch.object(mds.KafkaConnectMDSRequiresRelation, "__init__")
    @patch.object(mds.KafkaConnectMDSRequiresRelation, "unit",
                  new_callable=PropertyMock)
    # Mock RBAC given this test will assume it is all correctly set
    @patch.object(mds.KafkaConnectMDSRequiresRelation, "_check_rbac_enabled")
    @patch.object(mds.KafkaConnectMDSRequiresRelation,
                  "kafka_connect_telemetry_enabled")
    @patch.object(mds.KafkaConnectMDSRequiresRelation,
                  "kafka_connect_secret_enabled")
    @patch.object(mds.KafkaConnectMDSRequiresRelation, "super_user_list",
                  new_callable=PropertyMock)
    @patch.object(mds.KafkaConnectMDSRequiresRelation, "req_params",
                  new_callable=PropertyMock)
    @patch.object(mds.KafkaConnectMDSRequiresRelation, "model",
                  new_callable=PropertyMock)
    @patch.object(mds.KafkaConnectMDSRequiresRelation, "_post_request")
    def test_on_mds_relation_changed(self,
                                     mock_post_req,
                                     mock_model,
                                     mock_req_params,
                                     mock_super_users,
                                     mock_secrets_enabled,
                                     mock_telemetry_enabled,
                                     mock_check_rbac,
                                     mock_unit,
                                     mock_init,
                                     mock_get_cluster_id):
        # __init__ must return None
        mock_init.return_value = None
        mock_unit.return_value = unit(True)
        mock_telemetry_enabled.return_value = True
        mock_secrets_enabled.return_value = False
        mock_super_users.return_value = ["a", "b"]
        mock_get_cluster_id.return_value = "1234"
        mock_req_params.return_value = {
            "kafka_connect_cluster_name": "connect-test",
            "kafka-cluster-id": "1234",
            "group-id": "43",
            "kafka-hosts": {
                "host": "test1.example.org",
                "port": 123
            },
            "config-storage-topic": "config-storage-topic",
            "rest-advertised-protocol": "rest-advertised-protocol",
            "offset-storage-topic": "offset-storage-topic",
            "status-storage-topic": "status-storage-topic",
            "confluent-license-topic": "confluent-license-topic",
            "kafka_connect_ldap_user": "connect",
            "confluent.monitoring.interceptor.topic":
                "confluent.monitoring.interceptor.topic"
        }
        mock_model.return_value = self.MODEL
        rel = relation({
            self.APP_NAME: {
                "rbac_request_sent": "false"
            }
        })
        obj = mds.KafkaConnectMDSRequiresRelation(None, "kafka_connect")
        obj.on_mds_relation_changed(event(rel))
        mock_post_req.assert_any_call('/security/1.0/principals/User:a/roles/SystemAdmin', {'clusters': {'kafka-cluster': '1234', 'connect-cluster': '43'}}) # noqa
        mock_post_req.assert_any_call('/security/1.0/principals/User:b/roles/SystemAdmin', {'clusters': {'kafka-cluster': '1234', 'connect-cluster': '43'}}) # noqa
        mock_post_req.assert_any_call('/security/1.0/registry/clusters', [{'clusterName': 'connect-test', 'scope': {'clusters': {'kafka-cluster': '1234', 'connect-cluster': '43'}}, 'hosts': {'host': 'test1.example.org', 'port': 123}, 'protocol': 'rest-advertised-protocol'}]) # noqa
        mock_post_req.assert_any_call('/security/1.0/principals/User:connect/roles/SecurityAdmin', {'clusters': {'kafka-cluster': '1234', 'connect-cluster': '43'}}) # noqa
        mock_post_req.assert_any_call('/security/1.0/principals/User:connect/roles/ResourceOwner/bindings', {'scope': {'clusters': {'kafka-cluster': '1234'}, 'resourcePatterns': [{'resourceType': 'Group', 'name': '43', 'patternType': 'LITERAL'}, {'resourceType': 'Topic', 'name': 'config-storage-topic', 'patternType': 'LITERAL'}, {'resourceType': 'Topic', 'name': 'offset-storage-topic', 'patternType': 'LITERAL'}, {'resourceType': 'Topic', 'name': 'status-storage-topic', 'patternType': 'LITERAL'}, {'resourceType': 'Topic', 'name': 'confluent-license-topic', 'patternType': 'LITERAL'}]}}) # noqa
        mock_post_req.assert_any_call('/security/1.0/principals/User:connect/roles/ResourceOwner/bindings', {'scope': {'clusters': {'kafka-cluster': '1234'}, 'resourcePatterns': [{'resourceType': 'Group', 'name': '', 'patternType': 'LITERAL'}, {'resourceType': 'Topic', 'name': '', 'patternType': 'LITERAL'}]}}) # noqa
        mock_post_req.assert_any_call('/security/1.0/principals/User:connect/roles/DeveloperWrite/bindings', {'scope': {'clusters': {'kafka-cluster': '1234'}, 'resourcePatterns': [{'resourceType': 'Topic', 'name': 'confluent.monitoring.interceptor.topic', 'patternType': 'LITERAL'}]}}) # noqa
