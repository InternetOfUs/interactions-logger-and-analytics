# Memorable Experience - Logging and performances

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Setup and configuration](#setup-and-configuration)
  - [Required Python Packages](#required-python-packages)
  - [Environment variables](#environment-variables)
- [Usage](#usage)
  - [Web service](#web-service)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

There is a web service. It can be used to store, retrieve and delete messages (+ logs and performances, but not sure they work as documented or as should work).

The web service need an Elasticsearch database for storing data.

It could be used a Celery (a Distributed Task Queue) instance for handling the post request of analytics.
It requires a broker and a backend. An example to use it, having a Redis instance on the `localhost` on the port `6379` in db number `0`, is by setting the two env vars in this way:

```python
CELERY_BROKER_URL='redis://localhost:6379/0'
CELERY_RESULT_BACKEND='redis://localhost:6379/0'
```

There is also a script for computing the analytics using the logger web service and a task manager.
It stores them in a .csv file which name should change in order to save analytics computed in different days to avoid overwriting them.


## Setup and configuration

### Required Python Packages

Required Python packages can be installed using the command:

```bash
pip install -r requirements.txt
```

### Environment variables

The web service allows to set the following environment variables:

* `WS_HOST` (optional, the default value is `0.0.0.0`): the host where the web service is going to be available, it can also be set using the argument `-wh` or `--whost` when manually running the Python service;
* `WS_PORT`(optional, the default value is `80`): the port where the web service is going to be available, it can also be set using the argument `-wp` or `--wport` when manually running the Python service;
* `EL_HOST` (optional, the default value is `localhost`): the host where the Elasticsearch database is going to be available, it can also be set using the argument `-eh` or `--ehost` when manually running the Python service;
* `EL_PORT`(optional, the default value is `9200`): the port where the Elasticsearch database is going to be available, it can also be set using the argument `-ep` or `--eport` when manually running the Python service;
* `EL_USERNAME` (optional for versions of Elasticsearch < `7`): the username of the user to access the Elasticsearch database, it can also be set using the argument `-eh` or `--ehost` when manually running the Python service;
* `EL_PASSWORD` (optional for versions of Elasticsearch < `7`): the password of the user to access the Elasticsearch database, it can also be set using the argument `-ep` or `--eport` when manually running the Python service;
* `CELERY_BROKER_URL` (optional, the default value is `None`): the information about the broker to use the Celery instance, it must be in the following format: `redis://:password@hostname:port/db_number`;
* `CELERY_RESULT_BACKEND` (optional, the default value is `None`): the information about the result backend to use the Celery instance, it must be in the following format: `redis://:password@hostname:port/db_number`.


The script for computing the analytics allows to set the following environment variables:
* `ANALYTIC_CSV_FILE`: the path of the csv file where to store the analytics, it can also be set using the argument `-f` or `--file` when manually running the Python service;
* `LOGGER_HOST`: the host of the logger web service, it can also be set using the argument `-lh` or `--lhost` when manually running the Python service;
* `TASK_MANAGER_HOST`: the host of the task manager, it can also be set using the argument `-th` or `--thost` when manually running the Python service;
* `ANALYTIC_PROJECT`: the project for which to compute the analytics, it can also be set using the argument `-p` or `--project` when manually running the Python service;
* `ANALYTIC_RANGE`(optional, the default value is `30D`): The temporal range in which compute the analytics, it can also be set using the argument `-r` or `--range` when manually running the Python service;
* `APIKEY`: the apikey for accessing the services, it can also be set using the argument `-a` or `--apikey` when manually running the Python service;
* `APP_ID`: the id of the application in which compute the analytics, it can also be set using the argument `-i` or `--appid` when manually running the Python service;


## Usage

### Web service

This service can be run with the command:

```bash
python -m memex_logging.ws.main
```

### Script for computing the analytics

This service can be run with the command:

```bash
python -m memex_logging.compute_analytics.main
```
