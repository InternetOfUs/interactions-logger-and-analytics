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
import json
import logging.config
import os

import dateparser
import pandas as pd
import pytz
from wenet.interface.client import ApikeyClient
from wenet.interface.hub import HubInterface
from wenet.interface.profile_manager import ProfileManagerInterface
from wenet.interface.task_manager import TaskManagerInterface

from memex_logging.common.log.logging import get_logging_configuration
from memex_logging.common.model.analytic.time import FixedTimeWindow, MovingTimeWindow
from memex_logging.common.utils import Utils


logging.config.dictConfig(get_logging_configuration("apps_usage"))
logger = logging.getLogger("compute_analytics_questions_users.apps_usage")


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-uf", "--user_file", type=str, default=os.getenv("USER_FILE"), help="The path of csv/tsv file where to store the users apps usage")
    arg_parser.add_argument("-pf", "--profile_file", type=str, default=os.getenv("PROFILE_FILE"), help="The path of json file where to store the profiles without personal information")
    arg_parser.add_argument("-pnf", "--profile_new_file", type=str, default=os.getenv("PROFILE_NEW_FILE"), help="The path of csv/tsv file where to store the profiles without personal information in the new format")
    arg_parser.add_argument("-tnf", "--task_new_file", type=str, default=os.getenv("TASK_NEW_FILE"), help="The path of csv/tsv file where to store the tasks in the new format")
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
    task_manager_interface = TaskManagerInterface(client, args.instance)  # TODO extract tasks information
    hub_interface = HubInterface(client, args.instance)
    profile_manager_interface = ProfileManagerInterface(client, args.instance)

    name, extension = os.path.splitext(args.user_file)
    user_file = open(args.user_file, "w")
    if extension == ".csv":
        user_file_writer = csv.writer(user_file)
    elif extension == ".tsv":
        user_file_writer = csv.writer(user_file, delimiter="\t")
    else:
        logger.warning(f"For the user file you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the user file you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    # name, extension = os.path.splitext(args.profile_new_file)
    # profile_new_file = open(args.user_file, "w")
    # if extension == ".csv":
    #     profile_new_file_writer = csv.writer(profile_new_file)
    # elif extension == ".tsv":
    #     profile_new_file_writer = csv.writer(profile_new_file, delimiter="\t")
    # else:
    #     logger.warning(f"For the profile new file you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
    #     raise ValueError(f"For the profile new file you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

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
        user_file_writer.writerow(["from", creation_from])
        user_file_writer.writerow(["to", creation_to])
    elif args.range:
        time_range = MovingTimeWindow(args.range)
        creation_from, creation_to = Utils.extract_range_timestamps(time_range)
        user_file_writer.writerow(["from", creation_from])
        user_file_writer.writerow(["to", creation_to])
    else:
        creation_from = None
        creation_to = None

    user_profile_updates = []
    all_user_profile_updates = []
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
        all_user_profile_updates.append(row["wenet_id"])

    user_profile_failures = []
    all_user_profile_failures = []
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
            all_user_profile_failures.append(row["wenet_id"])

    app_ids = args.app_ids.split(";")
    apps = [hub_interface.get_app_details(app_id) for app_id in app_ids]
    apps_users = [hub_interface.get_user_ids_for_app(app_id, from_datetime=creation_from, to_datetime=creation_to) for app_id in app_ids]
    apps_all_users = [hub_interface.get_user_ids_for_app(app_id) for app_id in app_ids]
    ilog_ids = hub_interface.get_user_ids_for_app(args.ilog_id, from_datetime=creation_from, to_datetime=creation_to)
    ilog_all_ids = hub_interface.get_user_ids_for_app(args.ilog_id)
    survey_ids = hub_interface.get_user_ids_for_app(args.survey_id, from_datetime=creation_from, to_datetime=creation_to)
    survey_all_ids = hub_interface.get_user_ids_for_app(args.survey_id)
    tasks = []
    for app_id in app_ids:
        tasks.extend(task_manager_interface.get_all_tasks(app_id=app_id, creation_from=creation_from, creation_to=creation_to))  # TODO we need the different creations for each pilot?

    user_ids = []
    for app_users in apps_users:
        user_ids += app_users
    user_ids = set(user_ids + ilog_ids + survey_ids + user_profile_updates + user_profile_failures)
    user_file_writer.writerow(["total users", len(user_ids)])
    user_file_writer.writerow([])
    user_file_writer.writerow(["email", "registered to survey", "completed survey", "chatbot", "ilog"])

    users_info = []
    for user_id in user_ids:
        profile = profile_manager_interface.get_user_profile(user_id)
        chatbots = ""
        chatbot_ids = ""
        for i, app_users in enumerate(apps_all_users):
            if user_id in app_users:
                chatbots = chatbots + ";" + apps[i].name if chatbots else apps[i].name
                chatbot_ids = chatbot_ids + ";" + apps[i].app_id if chatbot_ids else apps[i].app_id

        users_info.append({
            "profile": profile,
            "survey": "yes" if user_id in survey_all_ids or user_id in all_user_profile_updates or user_id in all_user_profile_failures else "no",
            "completed_survey": "yes" if user_id in all_user_profile_updates or user_id in all_user_profile_failures else "no",
            "chatbots": chatbots,
            "chatbot_ids": chatbot_ids,
            "ilog": "yes" if user_id in ilog_all_ids else "no",
        })

    # profile_new_file_writer.writerow([
    #     "userid", "gender", "locale", "nationality", "creationTs", "lastUpdateTs", "appId", "dateOfBirth - year",
    #     "department", "degree_programme", "university", "accommodation", "excitement", "promotion", "existence",
    #     "suprapersonal", "interactive", "normative", "extraversion", "agreeableness", "conscientiousness",
    #     "neuroticism", "openness", "c_food", "c_eating", "c_lit", "c_creatlit", "c_perf_mus", "c_plays", "c_perf_plays",
    #     "c_musgall", "c_perf_art", "c_watch_sp", "c_ind_sp", "c_team_sp", "c_accom", "c_locfac", "u_active", "u_read",
    #     "u_essay", "u_org", "u_balance", "u_assess", "u_theory", "u_pract",
    #     # TODO before are related to profile, after related to tasks
    #     "answers_count", "arts_and_crafts_Q", "cinema_theatre_Q", "cultural_interests_Q", "food_and_cooking_Q",
    #     "life_ponders_Q", "local_things_Q", "local_university_Q", "music_Q", "physical_activity_Q", "studying_career_Q",
    #     "varia_misc_Q", "questions_count", "clicked_moreanswers", "count_Q+A"
    # ])

    users_info = sorted(users_info, key=lambda x: x["chatbots"], reverse=True)
    raw_profiles = []
    for user_info in users_info:
        user_file_writer.writerow([user_info["profile"].email, user_info["survey"], user_info["completed_survey"], user_info["chatbots"], user_info["ilog"]])
        # profile_new_file_writer.writerow([user_info["profile"].profile_id, user_info["profile"].gender.value, user_info["profile"].locale, user_info["profile"].nationality, user_info["profile"].creation_ts, user_info["profile"].last_update_ts, user_info["chatbot_ids"], user_info["profile"].date_of_birth.year,
        #                                   # TODO extract from competences/meanings/materials
        #                                   "department", "degree_programme", "university", "accommodation", "excitement", "promotion", "existence", "suprapersonal", "interactive", "normative", "extraversion", "agreeableness", "conscientiousness", "neuroticism", "openness", "c_food", "c_eating", "c_lit", "c_creatlit", "c_perf_mus", "c_plays", "c_perf_plays", "c_musgall", "c_perf_art", "c_watch_sp", "c_ind_sp", "c_team_sp", "c_accom", "c_locfac", "u_active", "u_read", "u_essay", "u_org", "u_balance", "u_assess", "u_theory", "u_pract",
        #                                   # TODO extract from tasks
        #                                   "answers_count", "arts_and_crafts_Q", "cinema_theatre_Q", "cultural_interests_Q", "food_and_cooking_Q", "life_ponders_Q", "local_things_Q", "local_university_Q", "music_Q", "physical_activity_Q", "studying_career_Q", "varia_misc_Q", "questions_count", "clicked_moreanswers", "count_Q+A"])
        if user_info["chatbot_ids"]:
            raw_profile = user_info["profile"].to_repr()
            raw_profile.pop("email")
            raw_profile.pop("phoneNumber")
            raw_profile.pop("relationships")
            raw_profile.pop("name")
            raw_profile["dateOfBirth"].pop("month")
            raw_profile["dateOfBirth"].pop("day")
            raw_profile["appId"] = user_info["chatbot_ids"]
            raw_profiles.append(raw_profile)

    data_frame = pd.DataFrame(data=raw_profiles)  # TODO handle nested keys if we want to use pandas
    data_frame = data_frame.rename(columns={})
    data_frame.to_csv(args.profile_new_file, index=False)

    user_file.close()
    # profile_new_file.close()

    profiles_file = open(args.profile_file, "w")
    json.dump(raw_profiles, profiles_file, ensure_ascii=False, indent=2)
    profiles_file.close()
