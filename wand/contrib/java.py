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

import logging

from ops.charm import CharmBase
from ops.framework import StoredState

from wand.security.ssl import genRandomPassword

from charmhelpers.fetch.ubuntu import apt_update
from charmhelpers.fetch.ubuntu import apt_install

logger = logging.getLogger(__name__)


__all__ = [
    'JavaCharmBase'
]


class JavaCharmBase(CharmBase):

    PACKAGE_LIST = {
        'openjdk-11-headless': ['openjdk-11-jre-headless']
    }

    # Extra packages that follow Java, e.g. openssl for cert generation
    EXTRA_PACKAGES = [
       'openssl'
    ]
    ks = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self.ks.set_default(ks_password=genRandomPassword())
        self.ks.set_default(ts_password=genRandomPassword())

    def install_packages(self, java_version='openjdk-11-headless'):
        apt_update()
        apt_install(self.PACKAGE_LIST[java_version] + self.EXTRA_PACKAGES)
