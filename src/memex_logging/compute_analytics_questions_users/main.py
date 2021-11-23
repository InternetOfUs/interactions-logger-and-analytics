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
from json import JSONDecodeError

from emoji import emojize
from wenet.interface.client import ApikeyClient
from wenet.interface.hub import HubInterface
from wenet.interface.incentive_server import IncentiveServerInterface
from wenet.interface.profile_manager import ProfileManagerInterface
from wenet.interface.task_manager import TaskManagerInterface

from memex_logging.common.log.logging import get_logging_configuration
from memex_logging.common.model.analytic.descriptor.count import UserCountDescriptor
from memex_logging.common.model.analytic.descriptor.segmentation import MessageSegmentationDescriptor
from memex_logging.common.model.analytic.result.count import CountResult
from memex_logging.common.model.analytic.result.segmentation import SegmentationResult
from memex_logging.common.model.message import Message
from memex_logging.common.model.analytic.time import MovingTimeWindow, FixedTimeWindow
from memex_logging.memex_logging_lib.logging_utils import LoggingUtility
from memex_logging.common.utils import Utils


logging.config.dictConfig(get_logging_configuration("compute_analytics"))
logger = logging.getLogger("compute_analytics_questions_users.main")

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
    arg_parser.add_argument("-af", "--analytic_file", type=str, default=os.getenv("ANALYTIC_FILE"), help="The path of csv/tsv file where to store analytics")
    arg_parser.add_argument("-qf", "--question_file", type=str, default=os.getenv("QUESTION_FILE"), help="The path of csv/tsv file where to store questions")
    arg_parser.add_argument("-uf", "--user_file", type=str, default=os.getenv("USER_FILE"), help="The path of csv/tsv file where to store users")
    arg_parser.add_argument("-tf", "--task_file", type=str, default=os.getenv("TASK_FILE"), help="The path of json file where to store the tasks")
    arg_parser.add_argument("-mf", "--message_file", type=str, default=os.getenv("MESSAGE_FILE"), help="The path of json file where to store the dump of messages")
    arg_parser.add_argument("-i", "--instance", type=str, default=os.getenv("INSTANCE", "https://wenet.u-hopper.com/dev"), help="The target WeNet instance")
    arg_parser.add_argument("-a", "--apikey", type=str, default=os.getenv("APIKEY"), help="The apikey for accessing the services")
    arg_parser.add_argument("-ai", "--app_id", type=str, default=os.getenv("APP_ID"), help="The id of the application in which compute the analytics")
    arg_parser.add_argument("-ii", "--ilog_id", type=str, default=os.getenv("ILOG_ID"), help="The id of the ilog application to check if the user has enabled it or not")
    arg_parser.add_argument("-si", "--survey_id", type=str, default=os.getenv("SURVEY_ID"), help="The id of the survey application to check if the user has enabled it or not")
    arg_parser.add_argument("-r", "--range", type=str, default=os.getenv("TIME_RANGE"), help="The temporal range in which compute the analytics")
    arg_parser.add_argument("-s", "--start", type=str, default=os.getenv("START_TIME"), help="The start time from which compute the analytics")
    arg_parser.add_argument("-e", "--end", type=str, default=os.getenv("END_TIME"), help="The end time up to which compute the analytics")
    args = arg_parser.parse_args()

    client = ApikeyClient(args.apikey)
    task_manager_interface = TaskManagerInterface(client, args.instance)
    hub_interface = HubInterface(client, args.instance)
    profile_manager_interface = ProfileManagerInterface(client, args.instance)
    incentive_server_interface = IncentiveServerInterface(client, args.instance)
    logger_operations = LoggingUtility(args.instance + "/logger", args.app_id, {"x-wenet-component-apikey": args.apikey})

    if args.start and args.end:
        time_range = FixedTimeWindow.from_isoformat(args.start, args.end)
    elif args.range:
        time_range = MovingTimeWindow(args.range)
    else:
        logger.warning(f"Nor time range or start and end time are defined")
        raise ValueError(f"Nor time range or start and end time are defined")

    creation_from, creation_to = Utils.extract_range_timestamps(time_range)

    # get analytics
    name, extension = os.path.splitext(args.analytic_file)
    analytics_file = open(args.analytic_file, "w")
    if extension == ".csv":
        analytics_file_writer = csv.writer(analytics_file)
    elif extension == ".tsv":
        analytics_file_writer = csv.writer(analytics_file, delimiter="\t")
    else:
        logger.warning(f"For the analytics, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the analytics, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    analytics_file_writer.writerow(["app id", args.app_id])
    analytics_file_writer.writerow(["from", creation_from])
    analytics_file_writer.writerow(["to", creation_to])
    analytics_file_writer.writerow([])
    analytics_file_writer.writerow(["metric", "count", "description"])

    total_users = UserCountDescriptor(time_range, args.app_id, "total")
    total_users_result = CountResult.from_repr(logger_operations.get_analytic_result(total_users))
    analytics_file_writer.writerow(["total users", total_users_result.count, "The total number of users of the application"])

    active_users = UserCountDescriptor(time_range, args.app_id, "active")
    active_users_result = CountResult.from_repr(logger_operations.get_analytic_result(active_users))
    analytics_file_writer.writerow(["active users", active_users_result.count, "The number of users who used the application"])

    engaged_users = UserCountDescriptor(time_range, args.app_id, "engaged")
    engaged_users_result = CountResult.from_repr(logger_operations.get_analytic_result(engaged_users))
    analytics_file_writer.writerow(["engaged users", engaged_users_result.count, "The number of users who received a notification from the platform (incentive, prompt, badge, ..)"])

    new_users = UserCountDescriptor(time_range, args.app_id, "new")
    new_users_result = CountResult.from_repr(logger_operations.get_analytic_result(new_users))
    analytics_file_writer.writerow(["new users", new_users_result.count, "The number of new users who activated the application during the period of this analysis"])

    segmentation_messages = MessageSegmentationDescriptor(time_range, args.app_id, "all")
    segmentation_messages_result = SegmentationResult.from_repr(logger_operations.get_analytic_result(segmentation_messages))

    total_messages = 0
    request_messages = 0
    response_messages = 0
    notification_messages = 0
    for segmentation in segmentation_messages_result.segments:
        total_messages += segmentation.count
        if TYPE_REQUEST_MESSAGE == segmentation.segmentation_type:
            request_messages = segmentation.count
        if TYPE_RESPONSE_MESSAGE == segmentation.segmentation_type:
            response_messages = segmentation.count
        if TYPE_NOTIFICATION_MESSAGE == segmentation.segmentation_type:
            notification_messages = segmentation.count

    segmentation_requests = MessageSegmentationDescriptor(time_range, args.app_id, "requests")
    segmentation_requests_result = SegmentationResult.from_repr(logger_operations.get_analytic_result(segmentation_requests))

    text_requests = 0
    action_requests = 0
    for segmentation in segmentation_requests_result.segments:
        if TYPE_TEXT_CONTENT_REQUEST == segmentation.segmentation_type:
            text_requests = segmentation.count
        if TYPE_ACTION_CONTENT_REQUEST == segmentation.segmentation_type:
            action_requests = segmentation.count

    analytics_file_writer.writerow(["total messages", total_messages, "The total number of messages exchanged"])
    analytics_file_writer.writerow(["messages from users", request_messages, "The number of messages sent by users (textual messages, clicked buttons and commands)"])
    analytics_file_writer.writerow(["user textual messages", text_requests, "The number of textual messages sent by the users"])
    analytics_file_writer.writerow(["user action messages", action_requests, "The number of action messages (buttons and commands) sent by the users"])
    analytics_file_writer.writerow(["messages from bot", response_messages, "The number of messages sent by the application"])
    analytics_file_writer.writerow(["messages from wenet", notification_messages, "The number of messages sent by the WeNet platform"])

    tasks = task_manager_interface.get_all_tasks(app_id=args.app_id, creation_from=creation_from, creation_to=creation_to)
    tasks_file = open(args.task_file, "w")
    json.dump([task.to_repr() for task in tasks], tasks_file, ensure_ascii=False, indent=2)  # TODO we should use json.loads and emojize where needed
    tasks_file.close()
    analytics_file_writer.writerow(["questions", len(tasks), "The number of questions asked by the users"])

    transactions = task_manager_interface.get_all_transactions(app_id=args.app_id, creation_from=creation_from, creation_to=creation_to)
    transaction_labels = [transaction.label for transaction in transactions]

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
    name, extension = os.path.splitext(args.question_file)
    questions_file = open(args.question_file, "w")
    if extension == ".csv":
        questions_file_writer = csv.writer(questions_file)
    elif extension == ".tsv":
        questions_file_writer = csv.writer(questions_file, delimiter="\t")
    else:
        logger.warning(f"For the questions, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the questions, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    questions_file_writer.writerow(["app id", args.app_id])
    questions_file_writer.writerow(["from", creation_from])
    questions_file_writer.writerow(["to", creation_to])
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

        try:
            question = json.loads(task.goal.name)
        except JSONDecodeError:
            question = task.goal.name

        if chosen_answer_id:
            try:
                answer = json.loads(id_answer_map[chosen_answer_id])
            except JSONDecodeError:
                answer = id_answer_map[chosen_answer_id]
        else:
            answer = None

        questions_file_writer.writerow([emojize(str(question), use_aliases=True), emojize(str(answer), use_aliases=True) if answer is not None else None])

    questions_file.close()

    # pilot users and associated cohorts
    name, extension = os.path.splitext(args.user_file)
    users_file = open(args.user_file, "w")
    if extension == ".csv":
        users_file_writer = csv.writer(users_file)
    elif extension == ".tsv":
        users_file_writer = csv.writer(users_file, delimiter="\t")
    else:
        logger.warning(f"For the users, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the users, you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    users_file_writer.writerow(["app id", args.app_id])
    users_file_writer.writerow(["from", creation_from])
    users_file_writer.writerow(["to", creation_to])
    user_ids = hub_interface.get_user_ids_for_app(args.app_id, from_datetime=creation_from, to_datetime=creation_to)
    users_file_writer.writerow(["total users", len(user_ids)])
    users_file_writer.writerow([])
    users_file_writer.writerow(["name", "surname", "email", "gender", "incentive cohort", "ilog", "survey"])

    cohorts = incentive_server_interface.get_cohorts()
    ilog_user_ids = hub_interface.get_user_ids_for_app(args.ilog_id)
    survey_user_ids = hub_interface.get_user_ids_for_app(args.survey_id)
    for user_id in user_ids:
        user = profile_manager_interface.get_user_profile(user_id)
        user_cohort = None
        for cohort in cohorts:
            if cohort.get("app_id") == args.app_id:
                if cohort.get("user_id") == user_id:
                    if cohort.get("cohort") == 0:
                        user_cohort = "badges"
                    elif cohort.get("cohort") == 1:
                        user_cohort = "incentives and badges"
                    break

        has_user_enabled_ilog = "yes" if user_id in ilog_user_ids else "no"
        has_user_enabled_survey = "yes" if user_id in survey_user_ids else "no"
        users_file_writer.writerow([user.name.first, user.name.last, user.email, user.gender.name.lower() if user.gender else None, user_cohort, has_user_enabled_ilog, has_user_enabled_survey])
        if not user.email:
            logger.warning(f"User [{user.profile_id}] does not have an associated email")

    users_file.close()

    # extracting messages dump
    messages = [Message.from_repr(message) for message in logger_operations.get_messages(creation_from, creation_to, max_size=10000)]
    while len(messages) < total_messages:
        message = messages.pop(-1)
        messages.extend([Message.from_repr(message) for message in logger_operations.get_messages(message.timestamp, creation_to, max_size=10000)])

    messages_file = open(args.message_file, "w")
    json.dump([message.to_repr() for message in messages], messages_file, ensure_ascii=False, indent=2)
    messages_file.close()
