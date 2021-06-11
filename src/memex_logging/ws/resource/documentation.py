# Copyright 2021 U-Hopper srl
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

import os

from flask import Response
from flask_restful import Resource


class DocumentationResourceBuilder(object):
    """
    Logic class used to create enable the endpoints. This class is used in ws.py
    """
    @staticmethod
    def routes():
        return [
            (Documentation, '/documentation', ())
        ]


class Documentation(Resource):
    """
    This class can be used to log a single log message into the elasticsearch instance
    """

    def get(self) -> Response:
        """
        Get the documentation in a raw format
        :return: the documentation in a raw format
        """
        text_response = ""

        f = open("%s/openapi.yaml" % os.getenv("DOCUMENTATION_PATH", "../../../documentation"), "r")
        for line in f:
            text_response += line + "\n"
        f.close()

        resp = Response(text_response, mimetype='text/plain', headers={"Access-Control-Allow-Origin": "*"})
        resp.status_code = 200
        return resp
