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
# from mock import patch
# from mock import PropertyMock

# from OpenSSL import crypto, SSL
# import jks

from .test_cert import UBUNTU_COM_CERT

from wand.security.ssl import _break_crt_chain
from wand.security.ssl import _check_file_exists
from wand.security.ssl import CreateKeystoreAndTrustore
from wand.security.ssl import generateSelfSigned
from wand.security.ssl import genRandomPassword


class TestSecurity(unittest.TestCase):

    def setUp(self):
        super(TestSecurity, self).setUp()

    def test_break_crt_chain(self):
        self.assertEqual(2, len(_break_crt_chain(UBUNTU_COM_CERT)))

    def test_gen_self_signed(self):
        generateSelfSigned("/tmp", "testcert")
        self.assertEqual(True, _check_file_exists("/tmp/testcert.crt"))
        self.assertEqual(True, _check_file_exists("/tmp/testcert.key"))

    def test_create_ks_ts(self):
        ks_pwd = genRandomPassword()
        ts_pwd = genRandomPassword()
        self.assertEqual(48, len(ks_pwd))
        crt, key = generateSelfSigned("/tmp", "testcert")
        CreateKeystoreAndTrustore("/tmp/testks.jks",
                                  "/tmp/testts.jks",
                                  True,
                                  ks_pwd, ts_pwd,
                                  crt, key,
                                  user=None)
        self.assertEqual(True, _check_file_exists("/tmp/testks.jks"))
        self.assertEqual(True, _check_file_exists("/tmp/testts.jks"))
