import unittest
from mock import (
    patch,
    PropertyMock
)

import wand.apps.relations.zookeeper as zkRelation


# Simple event object mock. Returns a dict if
# property relation is called
class MockHelper(object):

    def __init__(self, data):
        self._data = data

    @property
    def relation(self):
        return self._data

    @property
    def data(self):
        return self._data

    @property
    def units(self):
        return list(self._data.keys())


class ZookeeperRelationTest(unittest.TestCase):

    def setUp(self):
        super(ZookeeperRelationTest, self).setUp()

    @patch.object(zkRelation.ZookeeperProvidesRelation, "state",
                  new_callable=PropertyMock)
    # Need to replace __init__ because we do not want to go through
    # the entire logic of Harness and operator framework here
    @patch.object(zkRelation.ZookeeperProvidesRelation, "__init__")
    @patch.object(zkRelation.ZookeeperProvidesRelation, "_get_all_tls_cert",
                  new_callable=PropertyMock)
    @patch.object(zkRelation.ZookeeperProvidesRelation, "unit",
                  new_callable=PropertyMock)
    @patch.object(zkRelation.ZookeeperProvidesRelation, "advertise_addr",
                  new_callable=PropertyMock)
    @patch.object(zkRelation, "get_hostname")
    def test_zk_provides_changed(self,
                                 mock_get_hostname,
                                 mock_advertise_addr,
                                 mock_unit,
                                 mock_cert_gen,
                                 mock_init,
                                 mock_state):

        class mock_state_class(object):
            def __init__(self):
                self._data = ""

            @property
            def zk_list(self):
                return self._data

            @zk_list.setter
            def zk_list(self, x):
                self._data = x

        # Init should return None
        state = mock_state_class()
        mock_state.return_value = state
        mock_init.return_value = None
        mock_relation = MockHelper(data={
            "this": {},
            "other": {"endpoint": "1.1.1.1:2182"}
        })
        mock_unit.return_value = "this"
        mock_event = MockHelper(data=mock_relation)
        mock_get_hostname.return_value = "2.2.2.2"
        # Create the object and trigger the methods
        zk = zkRelation.ZookeeperProvidesRelation(None, "zookeeper")
        # Manually set values for init routine
        zk._hostname = None
        zk._port = 2182
        zk.on_zookeeper_relation_changed(mock_event)
        mock_get_hostname.assert_called()
        self.assertEqual(mock_state.return_value.zk_list,
                         "2.2.2.2:2182,1.1.1.1:2182")
