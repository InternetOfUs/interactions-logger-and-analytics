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

It is web service. It could be used to store, retrive and delete messages (+ logs and analytics/performances but not sure they work).

The web service need an Elasticsearch database for saving data.

It could be used a Celery (Distributed Task Queue) instance in posting the analytics.
It requires a broker and a backend. An example to use it, having a Redis instance on the `localhost` on the port `6379`, is by setting the two env vars in this way:

```python
CELERY_BROKER='redis://localhost:6379'
CELERY_RESULT_BACKEND='redis://localhost:6379'
```


## Setup and configuration

Please mark the `src` folder as source route.

### Required Python Packages

All required Python packages can be installed using the command:

```bash
pip install -r requirements.txt
```

### Environment variables

The web service allows to set the following environment variables:

- `WS_HOST` (optional, the default value is `0.0.0.0`): the host where the web service is going to be available, can be set also using the argument `-wh` or `--whost`;
- `WS_PORT`(optional, the default value is `80`): the port where the web service is going to be available, can be set also using the argument `-wp` or `--wport`;
- `EL_HOST` (optional, the default value is `localhost`): the host where the Elasticsearch database is going to be available, can be set also using the argument `-eh` or `--ehost`;
- `EL_PORT`(optional, the default value is `9200`): the port where the Elasticsearch database is going to be available, can be set also using the argument `-ep` or `--eport`;
- `EL_USERNAME` (optional for versions of Elasticsearch < `7`): the username of the user to access the Elasticsearch database;
- `EL_PASSWORD` (optional for versions of Elasticsearch < `7`): the password of the user to access the Elasticsearch database;
- `CELERY_BROKER` (optional, the default value is `None`): the information about the broker;
- `CELERY_RESULT_BACKEND` (optional, the default value is `None`): the information about the result backend.


## Usage

### Web service

This service can be run with the command:

```bash
python -m memex_logging.ws_memex.main
```
