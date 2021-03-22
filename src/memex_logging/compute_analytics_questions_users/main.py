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

from memex_logging.common.log.logging import get_logging_configuration
from memex_logging.common.model.analytic import DefaultTime, UserMetric, MessageMetric, CustomTime
from memex_logging.compute_analytics_questions_users.incentive_server_connector import IncentiveServerConnector
from memex_logging.compute_analytics_questions_users.profile_manager_connector import ProfileManagerConnector
from memex_logging.compute_analytics_questions_users.task_manager_connector import TaskManagerConnector
from memex_logging.memex_logging_lib.logging_utils import LoggingUtility
from memex_logging.utils.utils import Utils
from wenet.common.interface.hub import HubInterface


logging.config.dictConfig(get_logging_configuration("compute_analytics"))
logger = logging.getLogger("compute_analytics.main")

# message types
TYPE_REQUEST_MESSAGE = "request"
TYPE_RESPONSE_MESSAGE = "response"
TYPE_NOTIFICATION_MESSAGE = "notification"

# request message content types
TYPE_TEXT_CONTENT_REQUEST = "text"
TYPE_ACTION_CONTENT_REQUEST = "action"

# transaction labels
LABEL_CREATE_TASK_TRANSACTION = "CREATE_TASK"
LABEL_ANSWER_TRANSACTION = "answerTransaction"
LABEL_NOT_ANSWER_TRANSACTION = "notAnswerTransaction"
LABEL_REPORT_QUESTION_TRANSACTION = "reportQuestionTransaction"
LABEL_BEST_ANSWER_TRANSACTION = "bestAnswerTransaction"
LABEL_MORE_ANSWER_TRANSACTION = "moreAnswerTransaction"
LABEL_REPORT_ANSWER_TRANSACTION = "reportAnswerTransaction"


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-af", "--afile", type=str, default=os.getenv("ANALYTICS_FILE"), help="The path of csv/tsv file where to save analytics")
    arg_parser.add_argument("-qf", "--qfile", type=str, default=os.getenv("QUESTIONS_FILE"), help="The path of csv/tsv file where to save questions")
    arg_parser.add_argument("-uf", "--ufile", type=str, default=os.getenv("USERS_FILE"), help="The path of csv/tsv file where to save users")
    arg_parser.add_argument("-i", "--instance", type=str, default=os.getenv("INSTANCE", "https://wenet.u-hopper.com/dev"), help="The target WeNet instance")
    arg_parser.add_argument("-a", "--apikey", type=str, default=os.getenv("APIKEY"), help="The apikey for accessing the services")
    arg_parser.add_argument("-ai", "--appid", type=str, default=os.getenv("APP_ID"), help="The id of the application in which compute the analytics")
    arg_parser.add_argument("-il", "--ilog", type=str, default=os.getenv("ILOG"), help="The id of the ilog application to check if the user has enabled it or not")
    arg_parser.add_argument("-p", "--project", type=str, default=os.getenv("PROJECT"), help="The project for which to compute the analytics")
    arg_parser.add_argument("-r", "--range", type=str, default=os.getenv("TIME_RANGE", "30D"), help="The temporal range in which compute the analytics")
    arg_parser.add_argument("-s", "--start", type=str, default=os.getenv("START_TIME"), help="The start time from which compute the analytics")
    arg_parser.add_argument("-e", "--end", type=str, default=os.getenv("END_TIME"), help="The end time up to which compute the analytics")
    args = arg_parser.parse_args()

    logger_host = args.instance + "/logger"
    task_manager_host = args.instance + "/task_manager"
    hub_host = args.instance + "/hub/frontend"
    profile_manager_host = args.instance + "/profile_manager"
    incentive_server_host = args.instance + "/incentive_server"

    headers = {"x-wenet-component-apikey": args.apikey}
    operations = LoggingUtility(logger_host, args.project, headers)
    task_manager_connector = TaskManagerConnector(task_manager_host, args.apikey)
    hub_interface = HubInterface(hub_host)
    profile_manager_connector = ProfileManagerConnector(profile_manager_host, args.apikey)
    incentive_server_connector = IncentiveServerConnector(incentive_server_host, args.apikey)

    if args.start and args.end:
        time_range = CustomTime(args.start, args.end)
    else:
        time_range = DefaultTime(args.range)

    created_from, created_to = Utils.extract_range_timestamps(time_range.to_repr())
    created_from = datetime.fromisoformat(created_from)
    created_to = datetime.fromisoformat(created_to)

    name, extension = os.path.splitext(args.afile)
    analytics_file = open(args.afile, "w")
    if extension == ".csv":
        analytics_file_writer = csv.writer(analytics_file)
    elif extension == ".tsv":
        analytics_file_writer = csv.writer(analytics_file, delimiter="\t")
    else:
        logger.warning(f"For the analytics, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the analytics, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    analytics_file_writer.writerow(["app id", args.appid])
    analytics_file_writer.writerow(["project", args.project])
    analytics_file_writer.writerow(["from", created_from])
    analytics_file_writer.writerow(["to", created_to])
    analytics_file_writer.writerow([])
    analytics_file_writer.writerow(["metric", "count", "description"])

    total_users = UserMetric("u:total")
    total_users_result = operations.get_analytic(time_range, total_users)
    analytics_file_writer.writerow(["total users", total_users_result["count"], "The total number of users of the application"])

    active_users = UserMetric("u:active")
    active_users_result = operations.get_analytic(time_range, active_users)
    analytics_file_writer.writerow(["active users", active_users_result["count"], "The number of users who used the application"])

    engaged_users = UserMetric("u:engaged")
    engaged_users_result = operations.get_analytic(time_range, engaged_users)
    analytics_file_writer.writerow(["engaged users", engaged_users_result["count"], "The number of users who received a notification from the platform (incentive, prompt, badge, ..)"])

    new_users = UserMetric("u:new")
    new_users_result = operations.get_analytic(time_range, new_users)
    analytics_file_writer.writerow(["new users", new_users_result["count"], "The number of new users who activated the application during the period of this analysis"])

    segmentation_messages = MessageMetric("m:segmentation")
    segmentation_messages_result = operations.get_analytic(time_range, segmentation_messages)

    total_messages = 0
    for key in segmentation_messages_result["counts"]:
        total_messages += segmentation_messages_result["counts"][key]
    analytics_file_writer.writerow(["total messages", total_messages, "The total number of messages exchanged"])

    request_messages = 0
    if TYPE_REQUEST_MESSAGE in segmentation_messages_result["counts"]:
        request_messages = segmentation_messages_result["counts"][TYPE_REQUEST_MESSAGE]
    analytics_file_writer.writerow(["messages from users", request_messages, "The number of messages sent by users (textual messages, clicked buttons and commands)"])

    segmentation_requests = MessageMetric("r:segmentation")
    segmentation_requests_result = operations.get_analytic(time_range, segmentation_requests)

    text_requests = 0
    if TYPE_TEXT_CONTENT_REQUEST in segmentation_requests_result["counts"]:
        text_requests = segmentation_requests_result["counts"][TYPE_TEXT_CONTENT_REQUEST]
    analytics_file_writer.writerow(["user textual messages", text_requests, "The number of textual messages sent by the users"])

    action_requests = 0
    if TYPE_ACTION_CONTENT_REQUEST in segmentation_requests_result["counts"]:
        action_requests = segmentation_requests_result["counts"][TYPE_ACTION_CONTENT_REQUEST]
    analytics_file_writer.writerow(["user action messages", action_requests, "The number of action messages (buttons and commands) sent by the users"])

    response_messages = 0
    if TYPE_RESPONSE_MESSAGE in segmentation_messages_result["counts"]:
        response_messages = segmentation_messages_result["counts"][TYPE_RESPONSE_MESSAGE]
    analytics_file_writer.writerow(["messages from bot", response_messages, "The number of messages sent by the application"])

    notification_messages = 0
    if TYPE_NOTIFICATION_MESSAGE in segmentation_messages_result["counts"]:
        notification_messages = segmentation_messages_result["counts"][TYPE_NOTIFICATION_MESSAGE]
    analytics_file_writer.writerow(["messages from wenet", notification_messages, "The number of messages sent by the WeNet platform"])

    tasks = task_manager_connector.get_tasks(args.appid, created_from, created_to)
    analytics_file_writer.writerow(["questions", len(tasks), "The number of questions asked by the users"])

    transactions = task_manager_connector.get_transactions(args.appid, created_from, created_to)
    # file.writerow(["actions on questions and answers", len(transactions), "The total number of actions performed on questions and answers by the users"])

    transaction_labels = [transaction.label for transaction in transactions]

    # created_task_transactions = transaction_labels.count(LABEL_CREATE_TASK_TRANSACTION)
    # file.writerow(["created questions", created_task_transactions, "The number of questions created"])  # it should be equal to `questions`, it is a duplicate

    report_question_transactions = transaction_labels.count(LABEL_REPORT_QUESTION_TRANSACTION)
    analytics_file_writer.writerow(["question reports", report_question_transactions, "The number of reports created for questions (e.g. usually for malicious contents)"])

    not_answer_transactions = transaction_labels.count(LABEL_NOT_ANSWER_TRANSACTION)
    analytics_file_writer.writerow(["can't help", not_answer_transactions, "The number of times a user cannot answer a question (by clicking on the button “Can't help”)"])

    answer_transactions = transaction_labels.count(LABEL_ANSWER_TRANSACTION)
    analytics_file_writer.writerow(["answers", answer_transactions, "The number of answers provided by the users"])

    report_answer_transactions = transaction_labels.count(LABEL_REPORT_ANSWER_TRANSACTION)
    analytics_file_writer.writerow(["answer reports", report_answer_transactions, "The number of reports created for answers (e.g. usually for malicious contents)"])

    best_answer_transactions = transaction_labels.count(LABEL_BEST_ANSWER_TRANSACTION)
    analytics_file_writer.writerow(["approved answers", best_answer_transactions, "The number answers accepted as the correct/useful ones"])

    more_answer_transactions = transaction_labels.count(LABEL_MORE_ANSWER_TRANSACTION)
    analytics_file_writer.writerow(["ask more people", more_answer_transactions, "The number of times a user asked WeNet for more answers (by clicking on the button “Ask more people”)"])

    analytics_file.close()

    # all questions and their chosen answers
    name, extension = os.path.splitext(args.qfile)
    questions_file = open(args.qfile, "w")
    if extension == ".csv":
        questions_file_writer = csv.writer(questions_file)
    elif extension == ".tsv":
        questions_file_writer = csv.writer(questions_file, delimiter="\t")
    else:
        logger.warning(f"For the questions, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the questions, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    questions_file_writer.writerow(["app id", args.appid])
    questions_file_writer.writerow(["project", args.project])
    questions_file_writer.writerow(["from", created_from])
    questions_file_writer.writerow(["to", created_to])
    questions_file_writer.writerow(["total questions", len(tasks)])
    questions_file_writer.writerow([])
    questions_file_writer.writerow(["question", "answer"])

    for task in tasks:
        id_answer_map = {}
        chosen_answer_id = None
        for transaction in task.transactions:
            if transaction.label == LABEL_ANSWER_TRANSACTION:
                id_answer_map[transaction.id] = transaction.attributes.get("answer")
            if transaction.label == LABEL_BEST_ANSWER_TRANSACTION:
                chosen_answer_id = transaction.attributes.get("transactionId")
        questions_file_writer.writerow([task.goal.name, id_answer_map[chosen_answer_id] if chosen_answer_id else None])

    questions_file.close()

    # pilot users and associated cohorts
    name, extension = os.path.splitext(args.ufile)
    users_file = open(args.ufile, "w")
    if extension == ".csv":
        users_file_writer = csv.writer(users_file)
    elif extension == ".tsv":
        users_file_writer = csv.writer(users_file, delimiter="\t")
    else:
        logger.warning(f"For the users, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the users, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    users_file_writer.writerow(["app id", args.appid])
    users_file_writer.writerow(["project", args.project])
    user_ids = hub_interface.get_user_ids_for_app(args.appid)
    users_file_writer.writerow(["total users", len(user_ids)])
    users_file_writer.writerow([])
    users_file_writer.writerow(["name", "surname", "email", "gender", "incentive cohort", "ilog"])

    cohorts = incentive_server_connector.get_cohorts()
    ilog_user_ids = hub_interface.get_user_ids_for_app(args.ilog)
    for user_id in user_ids:
        user = profile_manager_connector.get_user_profile(user_id)
        user_cohort = None
        for cohort in cohorts:
            if cohort.get("app_id") == args.appid:
                if cohort.get("user_id") == user_id:
                    if cohort.get("cohort") == 0:
                        user_cohort = "badges"
                    elif cohort.get("cohort") == 1:
                        user_cohort = "incentives and badges"
                    break

        has_user_enabled_ilog = "no"
        for ilog_user_id in ilog_user_ids:
            if ilog_user_id == user_id:
                has_user_enabled_ilog = "yes"
                break

        users_file_writer.writerow([user.name.first, user.name.last, user.email, user.gender.name.lower() if user.gender else None, user_cohort, has_user_enabled_ilog])
        if not user.email:
            logger.warning(f"User [{user.profile_id}] does not have an associated email")

    users_file.close()
