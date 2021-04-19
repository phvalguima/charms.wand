import os
import unittest

import wand.contrib.linux as linux

ETCHOSTS="""# This is a comment
# And one more


127.0.0.1 nodetest
127.0.1.1 nodetest.maas
::1 ip6-localhost ip6-loopback
""" # noqa

FINALETCDHOSTS="""# This is a comment
# And one more


::1	ip6-localhost ip6-loopback
1.1.1.1	nodetest.maas
127.0.0.1	localhost
""" # noqa


class TestContribLinux(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        super(TestContribLinux, self).setUp()

    def test_fix_maybe_hosts(self):
        def __cleanup():
            try:
                os.remove("/tmp/3niofetchosts")
            except: # noqa
                pass

        __cleanup()
        with open("/tmp/3niofetchosts", "w") as f:
            f.write(ETCHOSTS)
            f.close()
        linux.fixMaybeLocalhost(
            hosts_path="/tmp/3niofetchosts",
            hostname="nodetest.maas",
            IP="1.1.1.1")
        result = ""
        with open("/tmp/3niofetchosts", "r") as f:
            result = f.read()
            f.close()
        self.assertEqual(result, FINALETCDHOSTS)
        __cleanup()
