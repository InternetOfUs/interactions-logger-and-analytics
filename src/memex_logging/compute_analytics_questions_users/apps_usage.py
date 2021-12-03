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

import argparse
import csv
import logging.config
import os

import dateparser
import pytz
from wenet.interface.client import ApikeyClient
from wenet.interface.hub import HubInterface
from wenet.interface.profile_manager import ProfileManagerInterface

from memex_logging.common.log.logging import get_logging_configuration
from memex_logging.common.model.analytic.time import FixedTimeWindow, MovingTimeWindow
from memex_logging.common.utils import Utils


logging.config.dictConfig(get_logging_configuration("apps_usage"))
logger = logging.getLogger("compute_analytics_questions_users.apps_usage")


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-o", "--output", type=str, default=os.getenv("OUTPUT_FILE"), help="The path of csv/tsv file where to store the users apps usage")
    arg_parser.add_argument("-i", "--instance", type=str, default=os.getenv("INSTANCE", "https://wenet.u-hopper.com/dev"), help="The target WeNet instance")
    arg_parser.add_argument("-a", "--apikey", type=str, default=os.getenv("APIKEY"), help="The apikey for accessing the services")
    arg_parser.add_argument("-ai", "--app_ids", type=str, default=os.getenv("APP_IDS"), help="The ids of the chatbots from which take the users. The ids should be separated by `;`")
    arg_parser.add_argument("-ii", "--ilog_id", type=str, default=os.getenv("ILOG_ID"), help="The id of the ilog application to check if the user has enabled it or not")
    arg_parser.add_argument("-si", "--survey_id", type=str, default=os.getenv("SURVEY_ID"), help="The id of the survey application to check if the user has enabled it or not")
    arg_parser.add_argument("-u", "--updates", type=str, default=os.getenv("USER_UPDATES_DUMP"), help="The path of csv/tsv file with the dump of the user that updated the profile")
    arg_parser.add_argument("-f", "--failures", type=str, default=os.getenv("USER_FAILURES_DUMP"), help="The path of csv/tsv file with the dump of the user profiles that failed to update the profile")
    arg_parser.add_argument("-r", "--range", type=str, default=os.getenv("TIME_RANGE"), help="The temporal range in which compute the analytics")
    arg_parser.add_argument("-s", "--start", type=str, default=os.getenv("START_TIME"), help="The start time from which compute the analytics")
    arg_parser.add_argument("-e", "--end", type=str, default=os.getenv("END_TIME"), help="The end time up to which compute the analytics")
    args = arg_parser.parse_args()

    client = ApikeyClient(args.apikey)
    hub_interface = HubInterface(client, args.instance)
    profile_manager_interface = ProfileManagerInterface(client, args.instance)

    name, extension = os.path.splitext(args.output)
    output_file = open(args.output, "w")
    if extension == ".csv":
        output_file_writer = csv.writer(output_file)
    elif extension == ".tsv":
        output_file_writer = csv.writer(output_file, delimiter="\t")
    else:
        logger.warning(f"For the output file you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the output file you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    name, extension = os.path.splitext(args.updates)
    user_updates_dump = open(args.updates, "r")
    if extension == ".csv":
        user_updates_dump_reader = csv.DictReader(user_updates_dump)
    elif extension == ".tsv":
        user_updates_dump_reader = csv.DictReader(user_updates_dump, delimiter="\t")
    else:
        logger.warning(f"For the user updates dump you should pass the path of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the user updates dump you should pass the path of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    if args.failures:
        name, extension = os.path.splitext(args.failures)
        user_failures_dump = open(args.failures, "r")
        if extension == ".csv":
            user_failures_dump_reader = csv.DictReader(user_failures_dump)
        elif extension == ".tsv":
            user_failures_dump_reader = csv.DictReader(user_failures_dump, delimiter="\t")
        else:
            logger.warning(f"For the user failures dump you should pass the path of the following type of file [.csv, .tsv], instead you pass [{extension}]")
            raise ValueError(f"For the user failures dump you should pass the path of the following type of file [.csv, .tsv], instead you pass [{extension}]")
    else:
        user_failures_dump_reader = None

    if args.start and args.end:
        time_range = FixedTimeWindow.from_isoformat(args.start, args.end)
        creation_from, creation_to = Utils.extract_range_timestamps(time_range)
        output_file_writer.writerow(["from", creation_from])
        output_file_writer.writerow(["to", creation_to])
    elif args.range:
        time_range = MovingTimeWindow(args.range)
        creation_from, creation_to = Utils.extract_range_timestamps(time_range)
        output_file_writer.writerow(["from", creation_from])
        output_file_writer.writerow(["to", creation_to])
    else:
        creation_from = None
        creation_to = None

    user_profile_updates = []
    timezone = pytz.timezone("UTC")
    for row in user_updates_dump_reader:
        update_date = dateparser.parse(row["last_update"]).astimezone(timezone)
        if creation_from and creation_to:
            if timezone.localize(creation_from) <= update_date <= timezone.localize(creation_to):
                user_profile_updates.append(row["wenet_id"])
        elif creation_from:
            if timezone.localize(creation_from) <= update_date:
                user_profile_updates.append(row["wenet_id"])
        elif creation_to:
            if update_date <= timezone.localize(creation_to):
                user_profile_updates.append(row["wenet_id"])
        else:
            user_profile_updates.append(row["wenet_id"])

    user_profile_failures = []
    if user_failures_dump_reader is not None:
        for row in user_failures_dump_reader:
            failure_date = dateparser.parse(row["failure_datetime"]).astimezone(timezone)
            if creation_from and creation_to:
                if timezone.localize(creation_from) <= failure_date <= timezone.localize(creation_to):
                    user_profile_failures.append(row["wenet_id"])
            elif creation_from:
                if timezone.localize(creation_from) <= failure_date:
                    user_profile_failures.append(row["wenet_id"])
            elif creation_to:
                if failure_date <= timezone.localize(creation_to):
                    user_profile_failures.append(row["wenet_id"])
            else:
                user_profile_failures.append(row["wenet_id"])

    app_ids = args.app_ids.split(";")
    apps = [hub_interface.get_app_details(app_id) for app_id in app_ids]
    apps_users = [hub_interface.get_user_ids_for_app(app_id, from_datetime=creation_from, to_datetime=creation_to) for app_id in app_ids]
    apps_all_users = [hub_interface.get_user_ids_for_app(app_id) for app_id in app_ids]
    ilog_ids = hub_interface.get_user_ids_for_app(args.ilog_id, from_datetime=creation_from, to_datetime=creation_to)
    ilog_all_ids = hub_interface.get_user_ids_for_app(args.ilog_id)
    survey_ids = hub_interface.get_user_ids_for_app(args.survey_id, from_datetime=creation_from, to_datetime=creation_to)
    survey_all_ids = hub_interface.get_user_ids_for_app(args.survey_id)

    user_ids = []
    for app_users in apps_users:
        user_ids += app_users
    user_ids = set(user_ids + ilog_ids + survey_ids + user_profile_updates + user_profile_failures)
    output_file_writer.writerow(["total users", len(user_ids)])
    output_file_writer.writerow([])
    output_file_writer.writerow(["email", "registered to survey", "completed survey", "chatbot", "ilog"])

    user_info = []
    for user_id in user_ids:
        user = profile_manager_interface.get_user_profile(user_id)
        chatbots = ""
        for i, app_users in enumerate(apps_all_users):
            if user_id in app_users:
                chatbots = chatbots + ";" + apps[i].name if chatbots else apps[i].name

        user_info.append({
            "mail": user.email,
            "survey": "yes" if user_id in survey_all_ids or user_id in user_profile_updates or user_id in user_profile_failures else "no",
            "completed_survey": "yes" if user_id in user_profile_updates or user_id in user_profile_failures else "no",
            "chatbot": chatbots,
            "ilog": "yes" if user_id in ilog_all_ids else "no"
        })

    user_info = sorted(user_info, key=lambda x: x["chatbot"], reverse=True)
    for user in user_info:
        output_file_writer.writerow([user["mail"], user["survey"], user["completed_survey"], user["chatbot"], user["ilog"]])

    output_file.close()
