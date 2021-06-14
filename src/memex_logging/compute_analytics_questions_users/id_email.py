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

from wenet.common.interface.client import ApikeyClient
from wenet.common.interface.component import ComponentInterface
from wenet.common.interface.hub import HubInterface
from wenet.common.interface.profile_manager import ProfileManagerInterface


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-f", "--file", type=str, default=os.getenv("FILE"), help="The path of csv/tsv file where to store id-email associations")
    arg_parser.add_argument("-i", "--instance", type=str, default=os.getenv("INSTANCE", ComponentInterface.DEVELOPMENT_INSTANCE), help="The target WeNet instance")
    arg_parser.add_argument("-a", "--apikey", type=str, default=os.getenv("APIKEY"), help="The apikey for accessing the services")
    arg_parser.add_argument("-ai", "--appid", type=str, default=os.getenv("APP_ID"), help="The id of the application from which take the users")
    args = arg_parser.parse_args()

    client = ApikeyClient(args.apikey)
    hub_interface = HubInterface(client, instance=args.instance)
    profile_manager_interface = ProfileManagerInterface(client, instance=args.instance)

    name, extension = os.path.splitext(args.file)
    file = open(args.file, "w")
    if extension == ".csv":
        file_writer = csv.writer(file)
    elif extension == ".tsv":
        file_writer = csv.writer(file, delimiter="\t")
    else:
        raise ValueError(f"you should pass the path of one of the following type of file [.csv, .tsv], instead you pass [{extension}]")

    file_writer.writerow(["app id", args.appid])
    user_ids = hub_interface.get_user_ids_for_app(args.appid)
    file_writer.writerow(["total users", len(user_ids)])
    file_writer.writerow([])
    file_writer.writerow(["id", "email"])

    for user_id in user_ids:
        user = profile_manager_interface.get_user_profile(user_id)
        file_writer.writerow([user.profile_id, user.email])

    file.close()
