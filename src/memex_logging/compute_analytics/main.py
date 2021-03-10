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
from memex_logging.compute_analytics.task_manager_connector import TaskManagerConnector
from memex_logging.memex_logging_lib.logging_utils import LoggingUtility
from memex_logging.utils.utils import Utils


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
    arg_parser.add_argument("-f", "--file", type=str, default=os.getenv("CSV_FILE"), help="The file where to save analytics")
    arg_parser.add_argument("-lh", "--lhost", type=str, default=os.getenv("LOGGER_HOST"), help="The host of the logger ws")
    arg_parser.add_argument("-th", "--thost", type=str, default=os.getenv("TASK_MANAGER_HOST"), help="The host of the task manager")
    arg_parser.add_argument("-a", "--apikey", type=str, default=os.getenv("APIKEY"), help="The apikey for accessing the services")
    arg_parser.add_argument("-i", "--appid", type=str, default=os.getenv("APP_ID"), help="The id of the application in which compute the analytics")
    arg_parser.add_argument("-p", "--project", type=str, default=os.getenv("PROJECT"), help="The project for which to compute the analytics")
    arg_parser.add_argument("-r", "--range", type=str, default=os.getenv("TIME_RANGE", "30D"), help="The temporal range in which compute the analytics")
    arg_parser.add_argument("-s", "--start", type=str, default=os.getenv("START_TIME"), help="The start time from which compute the analytics")
    arg_parser.add_argument("-e", "--end", type=str, default=os.getenv("END_TIME"), help="The end time up to which compute the analytics")
    args = arg_parser.parse_args()

    headers = {"x-wenet-component-apikey": args.apikey}
    operations = LoggingUtility(args.lhost, args.project, headers)
    task_manager_connector = TaskManagerConnector(args.thost, args.apikey)

    if args.start and args.end:
        time_range = CustomTime(args.start, args.end)
    else:
        time_range = DefaultTime(args.range)

    created_from, created_to = Utils.extract_range_timestamps(time_range.to_repr())
    created_from = datetime.fromisoformat(created_from)
    created_to = datetime.fromisoformat(created_to)

    name, extension = os.path.splitext(args.file)
    if extension == ".csv":
        file = csv.writer(open(args.file, "w"))
    elif extension == ".tsv":
        file = csv.writer(open(args.file, "w"), delimiter="\t")
    else:
        logger.warning(f"You should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"You should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    file.writerow(["app id", args.appid])
    file.writerow(["project", args.project])
    file.writerow(["from", created_from])
    file.writerow(["to", created_to])
    file.writerow([])
    file.writerow(["metric", "count", "description"])

    total_users = UserMetric("u:total")
    total_users_result = operations.get_analytic(time_range, total_users)
    file.writerow(["total users", total_users_result["count"], "The total number of users of the application"])

    active_users = UserMetric("u:active")
    active_users_result = operations.get_analytic(time_range, active_users)
    file.writerow(["active users", active_users_result["count"], "The number of users who used the application"])

    engaged_users = UserMetric("u:engaged")
    engaged_users_result = operations.get_analytic(time_range, engaged_users)
    file.writerow(["engaged users", engaged_users_result["count"], "The number of users who received a notification from the platform (incentive, prompt, badge, ..)"])

    new_users = UserMetric("u:new")
    new_users_result = operations.get_analytic(time_range, new_users)
    file.writerow(["new users", new_users_result["count"], "The number of new users who activated the application during the period of this analysis"])

    segmentation_messages = MessageMetric("m:segmentation")
    segmentation_messages_result = operations.get_analytic(time_range, segmentation_messages)

    total_messages = 0
    for key in segmentation_messages_result["counts"]:
        total_messages += segmentation_messages_result["counts"][key]
    file.writerow(["total messages", total_messages, "The total number of messages exchanged"])

    request_messages = 0
    if TYPE_REQUEST_MESSAGE in segmentation_messages_result["counts"]:
        request_messages = segmentation_messages_result["counts"][TYPE_REQUEST_MESSAGE]
    file.writerow(["messages from users", request_messages, "The number of messages sent by users (textual messages, clicked buttons and commands)"])

    segmentation_requests = MessageMetric("r:segmentation")
    segmentation_requests_result = operations.get_analytic(time_range, segmentation_requests)

    text_requests = 0
    if TYPE_TEXT_CONTENT_REQUEST in segmentation_requests_result["counts"]:
        text_requests = segmentation_requests_result["counts"][TYPE_TEXT_CONTENT_REQUEST]
    file.writerow(["user textual messages", text_requests, "The number of textual messages sent by the users"])

    action_requests = 0
    if TYPE_ACTION_CONTENT_REQUEST in segmentation_requests_result["counts"]:
        action_requests = segmentation_requests_result["counts"][TYPE_ACTION_CONTENT_REQUEST]
    file.writerow(["user action messages", action_requests, "The number of action messages (buttons and commands) sent by the users"])

    response_messages = 0
    if TYPE_RESPONSE_MESSAGE in segmentation_messages_result["counts"]:
        response_messages = segmentation_messages_result["counts"][TYPE_RESPONSE_MESSAGE]
    file.writerow(["messages from bot", response_messages, "The number of messages sent by the application"])

    notification_messages = 0
    if TYPE_NOTIFICATION_MESSAGE in segmentation_messages_result["counts"]:
        notification_messages = segmentation_messages_result["counts"][TYPE_NOTIFICATION_MESSAGE]
    file.writerow(["messages from wenet", notification_messages, "The number of messages sent by the WeNet platform"])

    tasks = task_manager_connector.get_tasks(args.appid, created_from, created_to)
    file.writerow(["questions ", len(tasks), "The number of questions asked by the users"])

    transactions = task_manager_connector.get_transactions(args.appid, created_from, created_to)
    # file.writerow(["actions on questions and answers", len(transactions), "The total number of actions performed on questions and answers by the users"])

    transaction_labels = [transaction.label for transaction in transactions]

    # created_task_transactions = transaction_labels.count(LABEL_CREATE_TASK_TRANSACTION)
    # file.writerow(["created questions", created_task_transactions, "The number of questions created"])  # it should be equal to `questions`, it is a duplicate

    report_question_transactions = transaction_labels.count(LABEL_REPORT_QUESTION_TRANSACTION)
    file.writerow(["question reports", report_question_transactions, "The number of reports created for questions (e.g. usually for malicious contents)"])

    not_answer_transactions = transaction_labels.count(LABEL_NOT_ANSWER_TRANSACTION)
    file.writerow(["can't help", not_answer_transactions, "The number of times a user cannot answer a question (by clicking on the button “Can't help”)"])

    answer_transactions = transaction_labels.count(LABEL_ANSWER_TRANSACTION)
    file.writerow(["answer", answer_transactions, "The number of answers provided by the users"])

    report_answer_transactions = transaction_labels.count(LABEL_REPORT_ANSWER_TRANSACTION)
    file.writerow(["answer reports", report_answer_transactions, "The number of reports created for answers (e.g. usually for malicious contents)"])

    best_answer_transactions = transaction_labels.count(LABEL_BEST_ANSWER_TRANSACTION)
    file.writerow(["approved answers", best_answer_transactions, "The number answers accepted as the correct/useful ones"])

    more_answer_transactions = transaction_labels.count(LABEL_MORE_ANSWER_TRANSACTION)
    file.writerow(["ask more people", more_answer_transactions, "The number of times a user asked WeNet for more answers (by clicking on the button “Ask more people”)"])
