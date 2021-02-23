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

import argparse
import csv
import json
import logging.config
import os
from time import sleep

import requests

from memex_logging.common.log.logging import get_logging_configuration


logging.config.dictConfig(get_logging_configuration("compute_analytics"))
logger = logging.getLogger("compute_analytics.main")


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-f", "--file", type=str, dest="file", default=os.getenv("ANALYTIC_FILE", "analytics.csv"), help="The file where to save analytics")
    arg_parser.add_argument("-ho", "--host", type=str, dest="host", default=os.getenv("ANALYTIC_HOST", "http://0.0.0.0:80"), help="The host of the ws to compute the analytics")
    arg_parser.add_argument("-p", "--project", type=str, dest="project", default=os.getenv("ANALYTIC_PROJECT", "wenet"), help="The project where to compute the analytics")
    arg_parser.add_argument("-r", "--range", type=str, dest="range", default=os.getenv("ANALYTIC_RANGE", "30D"), help="The temporal range in which compute the analytics")
    arg_parser.add_argument("-a", "--apikey", type=str, dest="apikey", default=os.getenv("APIKEY"), help="The apikey for accessing the ws")
    args = arg_parser.parse_args()

    headers = {"x-wenet-component-apikey": args.apikey}

    static_ids = []
    for dimension, metric in [("user", "u:total"), ("user", "u:active"), ("user", "u:engaged"), ("user", "u:new"), ("message", "m:from_user"), ("message", "m:responses"), ("message", "m:notifications")]:
        r = requests.post(args.host+"/analytic", headers=headers, json={
            "project": args.project,
            "timespan": {
                "type": "default",
                "value": args.range
            },
            "type": "analytic",
            "dimension": dimension,
            "metric": metric
        })

        static_ids.append(json.loads(r.content)["staticId"])

    sleep(10)

    result = []
    for (static_id, metric) in [(static_ids[0], "total users"), (static_ids[1], "active users"), (static_ids[2], "engaged users"), (static_ids[3], "new users"), (static_ids[4], "messages from user"), (static_ids[5], "response messages"), (static_ids[6], "notifications messages")]:
        r = requests.get(args.host + "/analytic", headers=headers, params={"staticId": static_id, "project": args.project})

        analytic = json.loads(r.content)["result"]
        analytic["metric"] = metric
        result.append(analytic)

    f = csv.writer(open(args.file, "w"))
    f.writerow(["metric", "count", "type"])

    for analytic in result:
        f.writerow([
            analytic["metric"],
            analytic["count"],
            analytic["type"]
        ])
