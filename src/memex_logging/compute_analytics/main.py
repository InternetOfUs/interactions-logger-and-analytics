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
import logging.config
import os
from datetime import datetime

from memex_logging.common.analytic.analytic import AnalyticComputation
from memex_logging.common.log.logging import get_logging_configuration
from memex_logging.common.model.analytic import DefaultTime, UserMetric, MessageMetric
from memex_logging.compute_analytics.task_manager_connector import TaskManagerConnector
from memex_logging.memex_logging_lib.logging_utils import LoggingUtility


logging.config.dictConfig(get_logging_configuration("compute_analytics"))
logger = logging.getLogger("compute_analytics.main")


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-f", "--file", type=str, dest="file", default=os.getenv("ANALYTIC_FILE", "analytics.csv"), help="The file where to save analytics")
    arg_parser.add_argument("-lh", "--lhost", type=str, dest="lhost", default=os.getenv("LOGGER_HOST", "http://0.0.0.0:80"), help="The host of the ws to compute the analytics")
    arg_parser.add_argument("-th", "--thost", type=str, dest="thost", default=os.getenv("TASK_MANAGER_HOST", "http://0.0.0.0:80"), help="The host of the ws to compute the analytics")
    arg_parser.add_argument("-p", "--project", type=str, dest="project", default=os.getenv("ANALYTIC_PROJECT", "wenet"), help="The project where to compute the analytics")
    arg_parser.add_argument("-r", "--range", type=str, dest="range", default=os.getenv("ANALYTIC_RANGE", "30D"), help="The temporal range in which compute the analytics")
    arg_parser.add_argument("-a", "--apikey", type=str, dest="apikey", default=os.getenv("APIKEY"), help="The apikey for accessing the ws")
    arg_parser.add_argument("-id", "--appid", type=str, dest="appid", default=os.getenv("APP_ID"), help="The id of the application in which compute the analytics")
    args = arg_parser.parse_args()

    headers = {"x-wenet-component-apikey": args.apikey}
    operations = LoggingUtility(args.lhost, args.project, headers)
    task_manager_connector = TaskManagerConnector(args.thost, args.apikey)

    time_range = DefaultTime(args.range)
    created_from, created_to = AnalyticComputation.support_bound_timestamp(time_range.to_repr())

    created_from = datetime.fromisoformat(created_from)
    created_to = datetime.fromisoformat(created_to)

    tasks = task_manager_connector.get_tasks(args.appid, created_from, created_to)
    transactions = task_manager_connector.get_transactions(args.appid, created_from, created_to)

    f = csv.writer(open(args.file, "w"))
    f.writerow(["metric", "count"])

    total_users = UserMetric("u:total")
    total_users_result = operations.get_statistic(time_range, total_users)
    f.writerow(["total users", total_users_result["count"]])

    active_users = UserMetric("u:active")
    active_users_result = operations.get_statistic(time_range, active_users)
    f.writerow(["active users", active_users_result["count"]])

    engaged_users = UserMetric("u:engaged")
    engaged_users_result = operations.get_statistic(time_range, engaged_users)
    f.writerow(["engaged users", engaged_users_result["count"]])

    new_users = UserMetric("u:new")
    new_users_result = operations.get_statistic(time_range, new_users)
    f.writerow(["new users", new_users_result["count"]])

    segmentation_messages = MessageMetric("m:segmentation")
    segmentation_messages_result = operations.get_statistic(time_range, segmentation_messages)
    if "request" in segmentation_messages_result["counts"]:
        f.writerow(["request messages", segmentation_messages_result["counts"]["request"]])
    else:
        f.writerow(["request messages", 0])
    if "response" in segmentation_messages_result["counts"]:
        f.writerow(["response messages", segmentation_messages_result["counts"]["response"]])
    else:
        f.writerow(["response messages", 0])
    if "notification" in segmentation_messages_result["counts"]:
        f.writerow(["notification messages", segmentation_messages_result["counts"]["notification"]])
    else:
        f.writerow(["notification messages", 0])

    segmentation_requests = MessageMetric("r:segmentation")
    segmentation_requests_result = operations.get_statistic(time_range, segmentation_requests)
    if "text" in segmentation_messages_result["counts"]:
        f.writerow(["text request messages", new_users_result["counts"]["text"]])
    else:
        f.writerow(["text request messages", 0])
    if "action" in segmentation_messages_result["counts"]:
        f.writerow(["action request messages", new_users_result["counts"]["action"]])
    else:
        f.writerow(["action request messages", 0])

    f.writerow(["created tasks", len(tasks)])
    f.writerow(["created transactions", len(transactions)])
