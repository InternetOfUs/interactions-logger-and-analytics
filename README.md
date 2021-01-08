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

It is web service. How does it work?

Need an Elasticsearch for starting and saving data.

Need a redis for celery?


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
- `EL_PASSWORD` (optional for versions of Elasticsearch < `7`): the password of the user to access the Elasticsearch database.


## Usage

### Web service

This service can be run with the command:

```bash
python -m memex_logging.ws_memex.main
```
