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

It is a web service. It can be used to store, retrieve and delete messages (+ logs and analytics/performances, but not sure they work as documented or as should work).

The web service need an Elasticsearch database for storing data.

It could be used a Celery (a Distributed Task Queue) instance for handling the post request of analytics.
It requires a broker and a backend. An example to use it, having a Redis instance on the `localhost` on the port `6379`, is by setting the two env vars in this way:

```python
CELERY_BROKER='redis://localhost:6379'
CELERY_RESULT_BACKEND='redis://localhost:6379'
```


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
* `CELERY_BROKER` (optional, the default value is `None`): the information about the broker to use the Celery instance, it must be in the following format: `<broker_name>://<host>:<port>`;
* `CELERY_RESULT_BACKEND` (optional, the default value is `None`): the information about the result backend to use the Celery instance, it must be in the following format: `<backend_result_name>://<host>:<port>`.


## Usage

### Web service

This service can be run with the command:

```bash
python -m memex_logging.ws.main
```
