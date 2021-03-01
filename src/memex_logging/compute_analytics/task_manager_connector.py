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

import json
from datetime import datetime
from typing import List

import requests

from wenet.common.model.task.task import TaskPage, Task
from wenet.common.model.task.transaction import TaskTransaction, TaskTransactionPage


class TaskManagerConnector:

    def __init__(self, host: str, apikey: str):
        self._host = host
        self._apikey = apikey

    def _create_apikey_header(self):
        return {"x-wenet-component-apikey": self._apikey}

    def get_tasks(self, app_id: str, created_from: datetime, created_to: datetime) -> List[Task]:
        tasks = []
        has_got_all_tasks = False
        offset = 0
        while not has_got_all_tasks:
            result = requests.get(self._host + "/tasks", headers=self._create_apikey_header(),
                                  params={"appId": app_id, "createdFrom": int(created_from.timestamp()),
                                          "createdTo": int(created_to.timestamp()), "offset": offset})

            if result.status_code == 200:
                task_page = TaskPage.from_repr(json.loads(result.content))
            else:
                raise Exception(f"request has return a code {result.status_code} with content {result.content}")

            tasks.extend(task_page.tasks)
            offset = len(tasks)
            if len(tasks) >= task_page.total:
                has_got_all_tasks = True

        return tasks

    def get_transactions(self, app_id: str, created_from: datetime, created_to: datetime) -> List[TaskTransaction]:
        transactions = []
        has_got_all_transactions = False
        offset = 0
        while not has_got_all_transactions:
            result = requests.get(self._host + "/taskTransactions", headers=self._create_apikey_header(),
                                  params={"appId": app_id, "createdFrom": int(created_from.timestamp()),
                                          "createdTo": int(created_to.timestamp()), "offset": offset})

            if result.status_code == 200:
                transaction_page = TaskTransactionPage.from_repr(json.loads(result.content))
            else:
                raise Exception(f"request has return a code {result.status_code} with content {result.content}")

            transactions.extend(transaction_page.transactions)
            offset = len(transactions)
            if len(transactions) >= transaction_page.total:
                has_got_all_transactions = True

        return transactions
