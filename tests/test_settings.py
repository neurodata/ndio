# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import sys

DEV_MODE = True

# server to check against

#HOSTNAME = 'localhost:8080'
HOSTNAME = 'localhost'
NEURODATA = open('/tmp/token_super', 'r').read().replace('\n', '')
TEST = open('/tmp/token_user', 'r').read().replace('\n', '')
