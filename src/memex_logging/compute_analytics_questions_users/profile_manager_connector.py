# Copyright 2020 U-Hopper srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, annotations

import json

import requests

from wenet.common.model.user.user_profile import WeNetUserProfile


class ProfileManagerConnector:

    def __init__(self, host: str, apikey: str):
        self._host = host
        self._apikey = apikey

    def _create_apikey_header(self):
        return {"x-wenet-component-apikey": self._apikey}

    def get_user_profile(self, user_id: str) -> WeNetUserProfile:
        result = requests.get(self._host + "/profiles/" + user_id, headers=self._create_apikey_header())

        if result.status_code == 200:
            return WeNetUserProfile.from_repr(json.loads(result.content))
        else:
            raise Exception(f"request has return a code {result.status_code} with content {result.content}")
