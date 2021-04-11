# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup
import os


version_file = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            'VERSION'))
with open(version_file) as v:
    VERSION = v.read().strip()


SETUP = {
    'name': "charms.wand",
    'version': VERSION,
    'author': "phvalguima",
    'url': "https://github.com/phvalguima/charms.wand",
    'packages': [
        "wand",
    ],
    'install_requires': [
        'ops',
        'pyOpenSSL',
        'netifaces',
        'charmhelpers'
    ],
    'scripts': [
    ],
    'license': "Apache License 2.0",
    'long_description': open('README.rst').read(),
    'description': 'Lib to help write charms with ops framework',
}

if __name__ == '__main__':
    setup(**SETUP)
