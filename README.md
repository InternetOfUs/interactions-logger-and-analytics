# Memorable Experience - Logging and performances

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Setup and configuration](#setup-and-configuration)
  - [Required Python Packages](#required-python-packages)
  - [Environment variables](#environment-variables)
- [Usage](#usage)
  - [Web service](#web-service)
  - [Script for computing the analytics](#script-for-computing-the-analytics)
  - [Script for getting the associations](#script-for-getting-the-associations)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

There is a web service. It can be used to store, retrieve and delete messages (+ logs and performances, but not sure they work as documented or as should work).

The web service need an Elasticsearch database for storing data.

It could be used a Celery (a Distributed Task Queue) instance for handling the post request of analytics.
It requires a broker and a backend. An example to use it, having a Redis instance on the `localhost` on the port `6379` in db number `0`, is by setting the two env vars in this way:

```python
CELERY_BROKER_URL='redis://localhost:6379/0'
CELERY_RESULT_BACKEND='redis://localhost:6379/0'
```

You'll need a worker to get things done, run the following command in a separate terminal tab:

```bash
celery worker -A memex_logging.celery.initialize.celery
```

Note that it requires all the environment variables of the web service to work.


There is also a script for computing the analytics, questions, users. Analytics, questions and users will be stored in dedicated .csv/.tsv files.
It also extracts the tasks and a dump of the messages using the logger, the task manager, the hub, the profile manager and the incentive server. The tasks and the messages will be stored in dedicated .json files.

Finally, there is a script for getting the association between the id and the email of users. This will be stored in a dedicated .csv/.tsv file.


## Setup and configuration

### Required Python Packages

Required Python packages can be installed using the command:

```bash
pip install -r requirements.txt
```

### Environment variables

The web service allows to set the following environment variables:

* `WS_HOST` (optional, the default value is `0.0.0.0`): the host where the web service is going to be available;
* `WS_PORT`(optional, the default value is `80`): the port where the web service is going to be available;
* `EL_HOST` (optional, the default value is `localhost`): the host where the Elasticsearch database is going to be available;
* `EL_PORT`(optional, the default value is `9200`): the port where the Elasticsearch database is going to be available;
* `EL_USERNAME` (optional for versions of Elasticsearch < `7`): the username of the user to access the Elasticsearch database;
* `EL_PASSWORD` (optional for versions of Elasticsearch < `7`): the password of the user to access the Elasticsearch database;
* `INSTANCE`: the host of target instance;
* `APIKEY`: the apikey for accessing the services;
* `CELERY_BROKER_URL` (optional, the default value is `None`): the information about the broker to use the Celery instance, it must be in the following format: `redis://:password@hostname:port/db_number`;
* `CELERY_RESULT_BACKEND` (optional, the default value is `None`): the information about the result backend to use the Celery instance, it must be in the following format: `redis://:password@hostname:port/db_number`.

Optionally is it possible to configure sentry in order to track any problem. Just set the following environment variables:

* `SENTRY_DSN` (optional) The data source name for sentry, if not set the project will not create any event
* `SENTRY_RELEASE` (optional) If set, sentry will associate the events to the given release
* `SENTRY_ENVIRONMENT` (optional) If set, sentry will associate the events to the given environment (ex. `production`, `staging`)


The script for computing the analytics, questions, users and for extracting the tasks and the messages allows to set the following environment variables:

* `ANALYTICS_FILE`: the path of the csv/tsv file where to store the analytics, it can also be set using the argument `-af` or `--afile` when manually running the Python service;
* `QUESTIONS_FILE`: the path of the csv/tsv file where to store the questions, it can also be set using the argument `-qf` or `--qfile` when manually running the Python service;
* `USERS_FILE`: the path of the csv/tsv file where to store the users, it can also be set using the argument `-uf` or `--ufile` when manually running the Python service;
* `TASK_FILE`: the path of the json file where to store the tasks, it can also be set using the argument `-tf` or `--tfile` when manually running the Python service;
* `DUMP_FILE`: the path of the json file where to store the dump of messages, it can also be set using the argument `-df` or `--dfile` when manually running the Python service;
* `INSTANCE` (optional, the default value is the development instance): the host of target instance, it can also be set using the argument `-i` or `--instance` when manually running the Python service;
* `APIKEY`: the apikey for accessing the services, it can also be set using the argument `-a` or `--apikey` when manually running the Python service;
* `APP_ID`: the id of the application in which compute the analytics, it can also be set using the argument `-ai` or `--appid` when manually running the Python service;
* `PROJECT`: the project for which to compute the analytics, it can also be set using the argument `-p` or `--project` when manually running the Python service;
* `TIME_RANGE` (optional, the default value is `30D`): The temporal range in which compute the analytics, the allowed values are ["TODAY", "1D", "7D", "10D", "30D"], it can also be set using the argument `-r` or `--range` when manually running the Python service.

Alternatively to the `TIME_RANGE` arbitrary start and end time could be set using the following environment variables:

* `START_TIME`: The start time from which compute the analytics, must be in iso format, it can also be set using the argument `-s` or `--start` when manually running the Python service;
* `END_TIME`: The end time up to which compute the analytics, must be in iso format, it can also be set using the argument `-e` or `--end` when manually running the Python service.


The script for getting the association between the id and the email of users allows to set the following environment variables:

* `FILE`: the path of the csv/tsv file where to store the id-email associations, it can also be set using the argument `-f` or `--file` when manually running the Python service;
* `INSTANCE` (optional, the default value is the development instance): the host of target instance, it can also be set using the argument `-i` or `--instance` when manually running the Python service;
* `APIKEY`: the apikey for accessing the services, it can also be set using the argument `-a` or `--apikey` when manually running the Python service;
* `APP_ID`: the id of the application in which compute the analytics, it can also be set using the argument `-ai` or `--appid` when manually running the Python service;


## Usage

### Web service

This service can be run with the command:

```bash
python -m memex_logging.ws.main
```

### Script for computing the analytics

This service can be run with the command:

```bash
python -m memex_logging.compute_analytics_questions_users.main
```

### Script for getting the associations

This service can be run with the command:

```bash
python -m memex_logging.compute_analytics_questions_users.id_email
```
