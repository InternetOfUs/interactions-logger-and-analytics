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
import os

import dateparser
import pytz
from wenet.interface.client import ApikeyClient
from wenet.interface.hub import HubInterface
from wenet.interface.profile_manager import ProfileManagerInterface

from memex_logging.common.model.analytic.time import FixedTimeWindow, MovingTimeWindow
from memex_logging.common.utils import Utils

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-f", "--file", type=str, default=os.getenv("FILE"), help="The path of csv/tsv file where to store id-email associations")
    arg_parser.add_argument("-i", "--instance", type=str, default=os.getenv("INSTANCE", "https://wenet.u-hopper.com/dev"), help="The target WeNet instance")
    arg_parser.add_argument("-a", "--apikey", type=str, default=os.getenv("APIKEY"), help="The apikey for accessing the services")
    arg_parser.add_argument("-aau", "--aau_id", type=str, default=os.getenv("AAU_BOT"), help="The id of the we@AAU chatbot from which take the users")
    arg_parser.add_argument("-lse", "--lse_id", type=str, default=os.getenv("LSE_BOT"), help="The id of the we@LSE chatbot from which take the users")
    arg_parser.add_argument("-num", "--num_id", type=str, default=os.getenv("NUM_BOT"), help="The id of the we@NUM chatbot from which take the users")
    arg_parser.add_argument("-uc", "--uc_id", type=str, default=os.getenv("UC_BOT"), help="The id of the we@UC chatbot from which take the users")
    arg_parser.add_argument("-unitn", "--unitn_id", type=str, default=os.getenv("UNITN_BOT"), help="The id of the we@UniTN chatbot from which take the users")
    arg_parser.add_argument("-ii", "--ilog_id", type=str, default=os.getenv("ILOG_ID"), help="The id of the ilog application to check if the user has enabled it or not")
    arg_parser.add_argument("-si", "--survey_id", type=str, default=os.getenv("SURVEY_ID"), help="The id of the survey application to check if the user has enabled it or not")
    arg_parser.add_argument("-d", "--dump", type=str, default=os.getenv("DUMP"), help="The path of csv file with the dump of the updated user profiles")
    arg_parser.add_argument("-r", "--range", type=str, default=os.getenv("TIME_RANGE", "30D"), help="The temporal range in which compute the analytics")
    arg_parser.add_argument("-s", "--start", type=str, default=os.getenv("START_TIME"), help="The start time from which compute the analytics")
    arg_parser.add_argument("-e", "--end", type=str, default=os.getenv("END_TIME"), help="The end time up to which compute the analytics")
    args = arg_parser.parse_args()

    client = ApikeyClient(args.apikey)
    hub_interface = HubInterface(client, args.instance)
    profile_manager_interface = ProfileManagerInterface(client, args.instance)

    name, extension = os.path.splitext(args.file)
    file = open(args.file, "w")
    if extension == ".csv":
        file_writer = csv.writer(file)
    elif extension == ".tsv":
        file_writer = csv.writer(file, delimiter="\t")
    else:
        logger.warning(f"you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    name, extension = os.path.splitext(args.dump)
    dump = open(args.dump, "r")
    if extension == ".csv":
        dump_reader = csv.DictReader(dump)
    else:
        raise ValueError(f"you should pass the path of the following type of file [.csv], instead you pass [{extension}]")

    if args.start and args.end:
        time_range = FixedTimeWindow.from_isoformat(args.start, args.end)
        creation_from, creation_to = Utils.extract_range_timestamps(time_range)
        file_writer.writerow(["from", creation_from])
        file_writer.writerow(["to", creation_to])
    elif args.range:
        time_range = MovingTimeWindow(args.range)
        creation_from, creation_to = Utils.extract_range_timestamps(time_range)
        file_writer.writerow(["from", creation_from])
        file_writer.writerow(["to", creation_to])
    else:
        creation_from = None
        creation_to = None

    user_profile_updates = []
    for row in dump_reader:
        timezone = pytz.timezone("UTC")
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

    aau_ids = hub_interface.get_user_ids_for_app(args.aau_id, from_datetime=creation_from, to_datetime=creation_to)
    lse_ids = hub_interface.get_user_ids_for_app(args.lse_id, from_datetime=creation_from, to_datetime=creation_to)
    num_ids = hub_interface.get_user_ids_for_app(args.num_id, from_datetime=creation_from, to_datetime=creation_to)
    uc_ids = hub_interface.get_user_ids_for_app(args.uc_id, from_datetime=creation_from, to_datetime=creation_to)
    unitn_ids = hub_interface.get_user_ids_for_app(args.unitn_id, from_datetime=creation_from, to_datetime=creation_to)
    ilog_ids = hub_interface.get_user_ids_for_app(args.ilog_id, from_datetime=creation_from, to_datetime=creation_to)
    survey_ids = hub_interface.get_user_ids_for_app(args.survey_id, from_datetime=creation_from, to_datetime=creation_to)
    user_ids = set(aau_ids + lse_ids + num_ids + uc_ids + unitn_ids + ilog_ids + survey_ids + user_profile_updates)
    user_ids = sorted(user_ids)
    file_writer.writerow(["total users", len(user_ids)])
    file_writer.writerow([])
    file_writer.writerow(["email", "registered to survey", "completed survey", "chatbot", "ilog"])

    user_info = []
    for user_id in user_ids:
        user = profile_manager_interface.get_user_profile(user_id)
        chatbot = ""
        if user_id in aau_ids:
            chatbot = chatbot + ":" + "we@AAU" if chatbot else "we@AAU"
        if user_id in lse_ids:
            chatbot = chatbot + ":" + "we@LSE" if chatbot else "we@LSE"
        if user_id in num_ids:
            chatbot = chatbot + ":" + "we@NUM" if chatbot else "we@NUM"
        if user_id in uc_ids:
            chatbot = chatbot + ":" + "we@UC" if chatbot else "we@UC"
        if user_id in unitn_ids:
            chatbot = chatbot + ":" + "we@UniTN" if chatbot else "we@UniTN"

        user_info.append({"mail": user.email, "survey": "yes" if user_id in survey_ids or user_id in user_profile_updates else "no", "completed_survey": "yes" if user_id in user_profile_updates else "no", "chatbot": chatbot, "ilog": "yes" if user_id in ilog_ids else "no"})

    user_info = sorted(user_info, key=lambda x: x["chatbot"], reverse=True)
    for user in user_info:
        file_writer.writerow([user["mail"], user["survey"], user["completed_survey"], user["chatbot"], user["ilog"]])

    file.close()
