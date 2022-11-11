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
import datetime
import logging.config
import os

from wenet.interface.client import ApikeyClient
from wenet.interface.hub import HubInterface
from wenet.interface.profile_manager import ProfileManagerInterface
from wenet.interface.task_manager import TaskManagerInterface
from wenet.model.user.competence import Competence
from wenet.model.user.material import Material
from wenet.model.user.meaning import Meaning

from memex_logging.common.log.logging import get_logging_configuration
from memex_logging.common.model.analytic.time import FixedTimeWindow, MovingTimeWindow
from memex_logging.common.utils import Utils
from memex_logging.compute_analytics_questions_users.utills import reconstruct_string


logging.config.dictConfig(get_logging_configuration("profiles_and_tasks_m46"))
logger = logging.getLogger("compute_analytics_questions_users.profiles_and_tasks_m46")

# callback message types
LABEL_QUESTION_TO_ANSWER_MESSAGE = "QuestionToAnswerMessage"
LABEL_QUESTION_EXPIRATION_MESSAGE = "QuestionExpirationMessage"
LABEL_ANSWERED_QUESTION_MESSAGE = "AnsweredQuestionMessage"
LABEL_ANSWERED_PICKED_MESSAGE = "AnsweredPickedMessage"

# transaction labels
LABEL_CREATE_TASK_TRANSACTION = "CREATE_TASK"
LABEL_ANSWER_TRANSACTION = "answerTransaction"
LABEL_BEST_ANSWER_TRANSACTION = "bestAnswerTransaction"
LABEL_MORE_ANSWER_TRANSACTION = "moreAnswerTransaction"


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-pf", "--profile_file", type=str, default=os.getenv("PROFILE_FILE"), help="The path of csv/tsv file where to store the profiles without personal information in the newly requested format for M46 pilots")
    arg_parser.add_argument("-tf", "--task_file", type=str, default=os.getenv("TASK_FILE"), help="The path of csv/tsv file where to store the tasks in the newly requested format for M46 pilots")
    arg_parser.add_argument("-i", "--instance", type=str, default=os.getenv("INSTANCE", "https://wenet.u-hopper.com/dev"), help="The target WeNet instance")
    arg_parser.add_argument("-a", "--apikey", type=str, default=os.getenv("APIKEY"), help="The apikey for accessing the services")
    arg_parser.add_argument("-ai", "--app_ids", type=str, default=os.getenv("APP_IDS"), help="The ids of the chatbots from which take the users. The ids should be separated by `;`")
    arg_parser.add_argument("-r", "--range", type=str, default=os.getenv("TIME_RANGE"), help="The temporal range in which compute the analytics")
    arg_parser.add_argument("-s", "--start", type=str, default=os.getenv("START_TIME"), help="The start time from which compute the analytics")
    arg_parser.add_argument("-e", "--end", type=str, default=os.getenv("END_TIME"), help="The end time up to which compute the analytics")
    args = arg_parser.parse_args()

    client = ApikeyClient(args.apikey)
    task_manager_interface = TaskManagerInterface(client, args.instance)
    hub_interface = HubInterface(client, args.instance)
    profile_manager_interface = ProfileManagerInterface(client, args.instance)

    name, extension = os.path.splitext(args.profile_file)
    profile_file = open(args.profile_file, "w")
    if extension == ".csv":
        profile_file_writer = csv.writer(profile_file)
    elif extension == ".tsv":
        profile_file_writer = csv.writer(profile_file, delimiter="\t")
    else:
        logger.warning(f"For the profile new file you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the profile new file you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    name, extension = os.path.splitext(args.task_file)
    task_file = open(args.task_file, "w")
    if extension == ".csv":
        task_file_writer = csv.writer(task_file)
    elif extension == ".tsv":
        task_file_writer = csv.writer(task_file, delimiter="\t")
    else:
        logger.warning(f"For the task new file you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")
        raise ValueError(f"For the task new file you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    if args.start and args.end:
        time_range = FixedTimeWindow.from_isoformat(args.start, args.end)
        creation_from, creation_to = Utils.extract_range_timestamps(time_range)
    elif args.range:
        time_range = MovingTimeWindow(args.range)
        creation_from, creation_to = Utils.extract_range_timestamps(time_range)
    else:
        creation_from = None
        creation_to = None

    app_ids = args.app_ids.split(";")
    apps = [hub_interface.get_app_details(app_id) for app_id in app_ids]
    user_ids = []
    all_user_ids = []
    apps_all_users = []
    tasks = []
    for app_id in app_ids:
        user_ids.extend(hub_interface.get_user_ids_for_app(app_id, from_datetime=creation_from, to_datetime=creation_to))  # TODO verify that this is correct
        apps_all_users.append(hub_interface.get_user_ids_for_app(app_id))
        all_user_ids.extend(hub_interface.get_user_ids_for_app(app_id))
        tasks.extend(task_manager_interface.get_all_tasks(app_id=app_id, creation_from=creation_from, creation_to=creation_to))
    user_ids = set(user_ids)
    all_user_ids = set(all_user_ids)

    task_file_writer.writerow([
        "id", "taskTypeId", "lastUpdateTs", "creationTs", "requesterId", "appId", "closeTs", "communityId",
        "transactions.taskId", "transactions.label", "transactions.creationTs", "transactions.actioneerId", "transactions.lastUpdateTs", "transactions.count.id",
        "transactions.messages.appId", "transactions.messages.receiverId", "transactions.messages.label", "transactions.messages.attributes.taskId",
        "transactions.messages.attributes.question", "transactions.messages.attributes.userId", "transactions.messages.attributes.anonymous", "transactions.messages.attributes.domain",
        "transactions.messages.attributes.positionOfAnswerer", "transactions.messages.attributes.transactionId", "transactions.messages.attributes.answer", "transactions.messages.attributes.listOfTransactionIds", "transactions.messages.attributes.title", "transactions.messages.attributes.text",
        "transactions.attributes.answer", "transactions.attributes.anonymous", "transactions.attributes.publish", "transactions.attributes.publishAnonymously", "transactions.attributes.reason", "transactions.attributes.transactionId", "transactions.attributes.expirationDate",
        "goal.name", "goal.description", "attributes.domain", "attributes.domainInterest", "attributes.beliefsAndValues", "attributes.anonymous",
        "attributes.socialCloseness", "attributes.positionOfAnswerer", "attributes.maxUsers", "attributes.maxAnswers", "attributes.expirationDate"
    ])

    # Build a dictionary {user: {key: value}} for having info per user
    task_user_info = {user_id: {
        "answers_count": 0,
        "academic_skills": 0,
        "basic_needs": 0,
        "physical_activity": 0,
        "appreciating_culture": 0,
        "random_thoughts": 0,
        "producing_culture": 0,
        "leisure_activities": 0,
        "campus_life": 0,
        "sensitive": 0,
        "questions_count": 0,
        "clicked_more_answers": 0,
        "count_Q+A": 0
    } for user_id in all_user_ids}

    for task in tasks:
        task.goal.name = reconstruct_string(task.goal.name)
        if task.requester_id in task_user_info and task.attributes.get("domain", "") in task_user_info[task.requester_id]:
            task_user_info[task.requester_id][task.attributes.get("domain", "")] += 1
            task_user_info[task.requester_id]["questions_count"] += 1
            task_user_info[task.requester_id]["count_Q+A"] += 1
        elif task.requester_id not in task_user_info:
            logger.warning(f"user [{task.requester_id}] that created the task [{task.task_id}] not in the users ids")
        elif task.attributes.get("domain", "") not in task_user_info[task.requester_id]:
            logger.warning(f"unknown domain [{task.attributes.get('domain', '')}] not among the task_user_info ones")

        for transaction in task.transactions:
            if transaction.label == LABEL_ANSWER_TRANSACTION and transaction.attributes.get("answer"):
                transaction.attributes["answer"] = reconstruct_string(transaction.attributes["answer"])
            if transaction.label == LABEL_BEST_ANSWER_TRANSACTION and transaction.attributes.get("reason"):
                transaction.attributes["reason"] = reconstruct_string(transaction.attributes["reason"])

            if transaction.label == LABEL_ANSWER_TRANSACTION:
                if transaction.actioneer_id in task_user_info:
                    task_user_info[transaction.actioneer_id]["answers_count"] += 1
                    task_user_info[transaction.actioneer_id]["count_Q+A"] += 1
                else:
                    logger.warning(f"user [{task.requester_id}] that answers to the task [{task.task_id}] not in the users ids")

            if transaction.label == LABEL_MORE_ANSWER_TRANSACTION:
                if transaction.actioneer_id in task_user_info:
                    task_user_info[transaction.actioneer_id]["clicked_more_answers"] += 1
                else:
                    logger.warning(f"user [{task.requester_id}] that asks more answers to the task [{task.task_id}] not in the users ids")

            messages_receivers = ""
            messages_label = ""
            messages_attributes_question = ""
            messages_attributes_user_id = ""
            messages_attributes_anonymous = ""
            messages_attributes_domain = ""
            messages_attributes_position_of_answerer = ""
            messages_attributes_transaction_id = ""
            messages_attributes_answer = ""
            messages_attributes_list_of_transaction_ids = ""
            messages_attributes_title = ""
            messages_attributes_text = ""
            for message in transaction.messages:
                if message.label in [LABEL_QUESTION_TO_ANSWER_MESSAGE, LABEL_ANSWERED_QUESTION_MESSAGE, LABEL_ANSWERED_PICKED_MESSAGE, LABEL_QUESTION_EXPIRATION_MESSAGE] and message.attributes.get("question"):
                    message.attributes["question"] = reconstruct_string(message.attributes["question"])
                if message.label == LABEL_ANSWERED_QUESTION_MESSAGE and message.attributes.get("answer"):
                    message.attributes["answer"] = reconstruct_string(message.attributes["answer"])

                if task.app_id != message.app_id:
                    logger.warning(f"message app_id [{message.app_id}] is different from the task app_id [{task.app_id}]")

                messages_receivers = message.receiver_id if not messages_receivers else messages_receivers + "|" + message.receiver_id

                if not messages_label:
                    messages_label = message.label
                elif message.label not in messages_label.split("|"):
                    messages_label = messages_label + "|" + message.label

                if message.attributes.get("taskId", "") and transaction.task_id != message.attributes.get("taskId", ""):
                    logger.warning(f"message app_id [{message.attributes.get('taskId', '')}] is different from the task app_id [{task.app_id}]")

                if not messages_attributes_question:
                    messages_attributes_question = message.attributes.get("question", "")
                elif message.attributes.get("question", "") and messages_attributes_question != message.attributes.get("question", ""):
                    logger.warning(f"message question [{message.attributes.get('question', '')}] is different from another message question of the same transaction [{messages_attributes_question}]")

                if not messages_attributes_user_id:
                    messages_attributes_user_id = message.attributes.get("userId", "")
                elif message.attributes.get("userId", "") and messages_attributes_user_id != message.attributes.get("userId", ""):
                    logger.warning(f"message user_id [{message.attributes.get('userId')}] is different from another message user_id of the same transaction [{messages_attributes_user_id}]")

                if not messages_attributes_anonymous:
                    messages_attributes_anonymous = message.attributes.get("anonymous", "")
                elif message.attributes.get("anonymous", "") and messages_attributes_anonymous != message.attributes.get("anonymous", ""):
                    logger.warning(f"message anonymous [{message.attributes.get('anonymous', '')}] is different from another message anonymous of the same transaction [{messages_attributes_anonymous}]")

                if not messages_attributes_domain:
                    messages_attributes_domain = message.attributes.get("domain", "")
                elif message.attributes.get("domain", "") and messages_attributes_domain != message.attributes.get("domain", ""):
                    logger.warning(f"message domain [{message.attributes.get('domain')}] is different from another message domain of the same transaction [{messages_attributes_domain}]")

                if not messages_attributes_position_of_answerer:
                    messages_attributes_position_of_answerer = message.attributes.get("positionOfAnswerer", "")
                elif message.attributes.get("positionOfAnswerer", "") and messages_attributes_position_of_answerer != message.attributes.get("positionOfAnswerer", ""):
                    logger.warning(f"message positionOfAnswerer [{message.attributes.get('positionOfAnswerer', '')}] is different from another message positionOfAnswerer of the same transaction [{messages_attributes_position_of_answerer}]")

                if not messages_attributes_transaction_id:
                    messages_attributes_transaction_id = message.attributes.get("transactionId", "")
                elif message.attributes.get("transactionId", "") and messages_attributes_transaction_id != message.attributes.get("transactionId", ""):
                    logger.warning(f"message transactionId [{message.attributes.get('transactionId', '')}] is different from another message transactionId of the same transaction [{messages_attributes_transaction_id}]")

                if not messages_attributes_answer:
                    messages_attributes_answer = message.attributes.get("answer", "")
                elif message.attributes.get("answer", "") and messages_attributes_answer != message.attributes.get("answer", ""):
                    logger.warning(f"message answer [{message.attributes.get('answer', '')}] is different from another message answer of the same transaction [{messages_attributes_answer}]")

                if not messages_attributes_list_of_transaction_ids:
                    messages_attributes_list_of_transaction_ids = message.attributes.get("listOfTransactionIds", "")
                elif message.attributes.get("listOfTransactionIds", "") and messages_attributes_list_of_transaction_ids != message.attributes.get("listOfTransactionIds", ""):
                    logger.warning(f"message listOfTransactionIds [{message.attributes.get('listOfTransactionIds', '')}] is different from another message listOfTransactionIds of the same transaction [{messages_attributes_list_of_transaction_ids}]")

                if not messages_attributes_title:
                    messages_attributes_title = message.attributes.get("title", "")
                elif message.attributes.get("title", "") and messages_attributes_title != message.attributes.get("title", ""):
                    logger.warning(f"message title [{message.attributes.get('title', '')}] is different from another message title of the same transaction [{messages_attributes_title}]")

                if not messages_attributes_text:
                    messages_attributes_text = message.attributes.get("text", "")
                elif message.attributes.get("text", "") and messages_attributes_text != message.attributes.get("text", ""):
                    logger.warning(f"message text [{message.attributes.get('text', '')}] is different from another message text of the same transaction [{messages_attributes_text}]")

            transaction_info = [
                transaction.task_id, transaction.label, datetime.datetime.fromtimestamp(transaction.creation_ts).strftime('%Y-%m-%d %H:%M:%S'), transaction.actioneer_id, datetime.datetime.fromtimestamp(transaction.last_update_ts).strftime('%Y-%m-%d %H:%M:%S'), transaction.id,
                task.app_id, messages_receivers, messages_label, transaction.task_id,
                messages_attributes_question, messages_attributes_user_id, messages_attributes_anonymous, messages_attributes_domain,
                messages_attributes_position_of_answerer, messages_attributes_transaction_id, messages_attributes_answer, "|".join(messages_attributes_list_of_transaction_ids), messages_attributes_title, messages_attributes_text,
                transaction.attributes.get("answer", ""), transaction.attributes.get("anonymous", ""), transaction.attributes.get("publish", ""), transaction.attributes.get("publishAnonymously", ""), transaction.attributes.get("reason", ""), transaction.attributes.get("transactionId", ""), datetime.datetime.fromtimestamp(transaction.attributes.get("expirationDate", "")) if transaction.attributes.get("expirationDate", "") else ""
            ]

            if transaction.label == LABEL_CREATE_TASK_TRANSACTION:
                task_file_writer.writerow([
                    task.task_id, task.task_type_id, datetime.datetime.fromtimestamp(task.last_update_ts).strftime('%Y-%m-%d %H:%M:%S') if task.last_update_ts else "", datetime.datetime.fromtimestamp(task.creation_ts).strftime('%Y-%m-%d %H:%M:%S') if task.creation_ts else "", task.requester_id, task.app_id, datetime.datetime.fromtimestamp(task.close_ts).strftime('%Y-%m-%d %H:%M:%S') if task.close_ts else "", task.community_id
                ] + transaction_info + [
                    task.goal.name, task.goal.description, task.attributes.get("domain", ""), task.attributes.get("domainInterest", ""), task.attributes.get("beliefsAndValues", ""), task.attributes.get("anonymous", ""),
                    task.attributes.get("socialCloseness", ""), task.attributes.get("positionOfAnswerer", ""), task.attributes.get("maxUsers", ""), task.attributes.get("maxAnswers", ""), datetime.datetime.fromtimestamp(task.attributes.get("expirationDate", "")).strftime('%Y-%m-%d %H:%M:%S') if task.attributes.get("expirationDate", "") else ""
                ])
            else:
                task_file_writer.writerow([
                    "", "", "", "", "", "", "", ""
                ] + transaction_info + [
                    "", "", "", "", "", "",
                    "", "", "", "", ""
                ])

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
            "chatbots": chatbots,
            "chatbot_ids": chatbot_ids
        })

    profile_file_writer.writerow([
        "userid", "gender", "locale", "nationality", "creationTs", "lastUpdateTs", "appId", "dateOfBirth - year",
        "department", "study_year", "university", "accommodation", "excitement", "promotion", "existence",
        "suprapersonal", "interactive", "normative", "extraversion", "agreeableness", "conscientiousness",
        "neuroticism", "openness", "c_food", "c_eating", "c_lit", "c_creatlit", "c_perf_mus", "c_plays", "c_perf_plays",
        "c_musgall", "c_perf_art", "c_watch_sp", "c_ind_sp", "c_team_sp", "c_accom", "c_locfac", "u_active", "u_read",
        "u_essay", "u_org", "u_balance", "u_assess", "u_theory", "u_pract",
        "answers_count", "academic_skills_Q", "basic_needs_Q", "physical_activity_Q", "appreciating_culture_Q",
        "random_thoughts_Q", "producing_culture_Q", "leisure_activities_Q", "campus_life_Q", "sensitive_Q"
        "questions_count", "clicked_more_answers", "count_Q+A"
    ])

    users_info = sorted(users_info, key=lambda x: x["chatbots"], reverse=True)
    raw_profiles = []
    for user_info in users_info:
        department = ""
        study_year = ""
        university = ""
        accommodation = ""
        excitement = ""
        promotion = ""
        existence = ""
        suprapersonal = ""
        interactive = ""
        normative = ""
        extraversion = ""
        agreeableness = ""
        conscientiousness = ""
        neuroticism = ""
        openness = ""
        c_food = ""
        c_eating = ""
        c_lit = ""
        c_creatlit = ""
        c_perf_mus = ""
        c_plays = ""
        c_perf_plays = ""
        c_musgall = ""
        c_perf_art = ""
        c_watch_sp = ""
        c_ind_sp = ""
        c_team_sp = ""
        c_accom = ""
        c_locfac = ""
        u_active = ""
        u_read = ""
        u_essay = ""
        u_org = ""
        u_balance = ""
        u_assess = ""
        u_theory = ""
        u_pract = ""
        for competence in user_info["profile"].competences:
            competence = Competence.from_repr(competence)
            if competence.name == "c_food":
                c_food = competence.level
            elif competence.name == "c_eating":
                c_eating = competence.level
            elif competence.name == "c_lit":
                c_lit = competence.level
            elif competence.name == "c_creatlit":
                c_creatlit = competence.level
            elif competence.name == "c_perf_mus":
                c_perf_mus = competence.level
            elif competence.name == "c_plays":
                c_plays = competence.level
            elif competence.name == "c_perf_plays":
                c_perf_plays = competence.level
            elif competence.name == "c_musgall":
                c_musgall = competence.level
            elif competence.name == "c_perf_art":
                c_perf_art = competence.level
            elif competence.name == "c_watch_sp":
                c_watch_sp = competence.level
            elif competence.name == "c_ind_sp":
                c_ind_sp = competence.level
            elif competence.name == "c_team_sp":
                c_team_sp = competence.level
            elif competence.name == "c_accom":
                c_accom = competence.level
            elif competence.name == "c_locfac":
                c_locfac = competence.level
            elif competence.name == "u_active":
                u_active = competence.level
            elif competence.name == "u_read":
                u_read = competence.level
            elif competence.name == "u_essay":
                u_essay = competence.level
            elif competence.name == "u_org":
                u_org = competence.level
            elif competence.name == "u_balance":
                u_balance = competence.level
            elif competence.name == "u_assess":
                u_assess = competence.level
            elif competence.name == "u_theory":
                u_theory = competence.level
            elif competence.name == "u_pract":
                u_pract = competence.level

        for meaning in user_info["profile"].meanings:
            meaning = Meaning.from_repr(meaning)
            if meaning.name == "excitement":
                excitement = meaning.level
            if meaning.name == "promotion":
                promotion = meaning.level
            if meaning.name == "existence":
                existence = meaning.level
            if meaning.name == "suprapersonal":
                suprapersonal = meaning.level
            if meaning.name == "interactive":
                interactive = meaning.level
            if meaning.name == "normative":
                normative = meaning.level
            if meaning.name == "extraversion":
                extraversion = meaning.level
            if meaning.name == "agreeableness":
                agreeableness = meaning.level
            if meaning.name == "conscientiousness":
                conscientiousness = meaning.level
            if meaning.name == "neuroticism":
                neuroticism = meaning.level
            if meaning.name == "openness":
                openness = meaning.level

        for material in user_info["profile"].materials:
            material = Material.from_repr(material)
            if material.name == "department":
                department = material.description
            if material.name == "study_year":
                study_year = material.description
            if material.name == "university":
                university = material.description
            if material.name == "accommodation":
                accommodation = material.description

        task_info = task_user_info[user_info["profile"].profile_id]
        task_info = {info: task_info[info] if task_info[info] else "" for info in task_info}

        profile_file_writer.writerow([
            user_info["profile"].profile_id, user_info["profile"].gender.value if user_info["profile"].gender else "", user_info["profile"].locale, user_info["profile"].nationality, user_info["profile"].creation_ts, user_info["profile"].last_update_ts, user_info["chatbot_ids"], user_info["profile"].date_of_birth.year,
            department, study_year, university, accommodation, excitement, promotion, existence, suprapersonal,
            interactive, normative, extraversion, agreeableness, conscientiousness, neuroticism, openness, c_food,
            c_eating, c_lit, c_creatlit, c_perf_mus, c_plays, c_perf_plays, c_musgall, c_perf_art, c_watch_sp, c_ind_sp,
            c_team_sp, c_accom, c_locfac, u_active, u_read, u_essay, u_org, u_balance, u_assess, u_theory, u_pract,
            task_info["answers_count"], task_info["academic_skills"], task_info["basic_needs"], task_info["physical_activity"], task_info["appreciating_culture"],
            task_info["random_thoughts"], task_info["producing_culture"], task_info["leisure_activities"], task_info["campus_life"], task_info["sensitive"],
            task_info["questions_count"], task_info["clicked_more_answers"], task_info["count_Q+A"]
        ])

    profile_file.close()
    task_file.close()
