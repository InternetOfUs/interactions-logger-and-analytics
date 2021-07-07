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

import abc
import importlib.util
from datetime import datetime
from inspect import isclass
from pkgutil import iter_modules

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

import argparse
import os
import logging
from typing import Dict


logging.basicConfig(level=logging.INFO)


class MigrationAction:

    @abc.abstractmethod
    def apply(self, es: Elasticsearch) -> None:
        pass

    @property
    @abc.abstractmethod
    def action_name(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def action_num(self) -> int:
        pass

    @property
    def unique_id(self) -> str:
        return f"{self.action_num}-{self.action_name}"

    def __repr__(self) -> str:
        return self.unique_id

    def __str__(self) -> str:
        return self.__repr__()


class MigrationManager:

    def __init__(self, elasticsearch_handler, manager_index: str):
        self._el_handler: Elasticsearch = elasticsearch_handler
        self._manager_index = manager_index
        self._actions: Dict[int, MigrationAction] = {}

    def with_migration_action(self, action: MigrationAction) -> MigrationManager:
        if action.action_num in self._actions:
            raise RuntimeError(f"Multiple action registered with number [{action.action_num}]")

        self._actions[action.action_num] = action
        return self

    def _need_to_apply_migration(self, action: MigrationAction) -> bool:
        try:
            self._el_handler.get(id=action.unique_id, index=self._manager_index)
            return False
        except NotFoundError:
            return True

    def _mark_as_applied(self, action: MigrationAction) -> None:
        body = {
            "name": action.action_name,
            "ts": datetime.now().isoformat()
        }
        self._el_handler.index(body=body, index=self._manager_index, id=action.unique_id)

    def apply_migrations(self) -> None:
        actions = [x for x in self._actions.values()]
        actions.sort(key=lambda x: x.action_num)
        for action in actions:
            if self._need_to_apply_migration(action):
                logging.info(f"Applying migration [{action.action_name}]")
                action.apply(self._el_handler)
                self._mark_as_applied(action)
            else:
                logging.info(f"Migration [{action.action_name}] already applied")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Elasticsearch Migrator")
    arg_parser.add_argument("-f", "--folder", default=os.getenv("MIGRATION_FOLDER"), type=str, help="Migration folder")
    arg_parser.add_argument("-i", "--index", default=os.getenv("MIGRATION_MANAGER_INDEX", "migrations"), type=str, help="Migration manager index")

    args = arg_parser.parse_args()
    folder = args.folder

    if folder is None:
        raise RuntimeError("The source folder must not be None")

    manager = MigrationManager(Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None))), args.index)

    logging.info(f"Loading migration from [{folder}]")
    for (module_path, module_name, is_package) in iter_modules([folder]):
        # import the module and iterate through its attributes
        if not is_package:
            spec = importlib.util.spec_from_file_location(module_name, f"{folder}/{module_name}.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            for attribute_name in dir(module):
                attribute = getattr(module, attribute_name)
                if isclass(attribute):
                    if attribute_name.endswith("Migration"):
                        migration_action = attribute()
                        manager.with_migration_action(migration_action)
                        logging.info(f"Loaded migration {migration_action.unique_id}")

    manager.apply_migrations()
