# Changelog

## Version 1.*

### NEXT

* Renamed time window type values
* Added periodically re-computations of moving time window analytics

### 1.4.0

* Imported wenet common models `2.0.0`.
* Improved script for computing analytics.
* Allowing to export the list of both tasks and messages in dedicated json files.
* Added new script for getting the association between user ids and their emails.
* Added Sentry integration.
* Integrated celery worker
* Updated documentation
* Refactored analytics models and analytics computations
* Added task and transaction analytics

### 1.3.0

* Added project filter into queries for message endpoint and other minor fixes
* Improved script for analytics and added all questions and their chosen answers and users and associated cohorts

### 1.2.1

* Updated the script for storing analytics on csv
* Updated wenet-common-models
* Updated queries for analytics including project keyword

### 1.2.0

* Updated message and user metrics
* Fixed aggregation query for user and message metrics

### 1.1.0

* Enabled Gunicorn for docker deployment

### 1.0.0

* Re-organised code for message management
* Added new endpoint for searching messages according to various parameters
* Applied project template `3.0.3`

## Version 0.*

### 0.2.0

* added apache licence specification

### 0.1.7

* added aggregation

### 0.1.6

* analytics and documentation improvements

### 0.1.5

* indices of logs and messages improvements

### 0.1.4

* documentation is up to date

### 0.1.3

* documentation is up to date

### 0.1.2

* click counters available

### 0.1.1

* notification works

### 0.1.0

* analytics, logging and documentation improvements
* MultiAction responses are supported

### 0.0.25

* carousel documentation updated

### 0.0.24

* documentation updated

### 0.0.23

* entities bug in request messages solved

### 0.0.22

* logs are now working
* the library support logs and notifications
* the models up-to date
* documentation up-to date

### 0.0.21

* new log data model
* new log APIs and adjustments to the CarouselResponse model
* documentation up-to date

### 0.0.20

* date parsing is working

### 0.0.19

* new approach to retrieve the messages
* location is now supported
* new approach to the timestamp management

### 0.0.18

* structure_id should be in the metadata
* timestamp is again an ISO text data

### 0.0.17

* small adjustments in the message logging APIs

### 0.0.16

* library update, data model update and utils update

### 0.0.15

* updated version tag in the build.sh file of the docker-support

### 0.0.14

* entities optional

### 0.0.13

* intent and domain are optional parameters

### 0.0.12

* bug fixes
* data model - Notification

### 0.0.11

* updated version tag in the build.sh file of the docker-support

### 0.0.10

* fixed documentation endpoint
* updated logging library, MPS1726

### 0.0.9

* updated logging library

### 0.0.8

* documentation folder loaded by build.sh

### 0.0.7

* new library with better examples + bug corrected in messaging APIs

### 0.0.6

* new data models for messages
* new data models for logging
* alpha version data model of analytics
* end-point for the documentation
* new version of the messages APIs

### 0.0.5

* minor adjustments
* default value of the project is memex. Whenever a message without the project value arrives, it is assigned to memex automatically
* utility package to use the APIs is now part of the release. The utilities can be found in `memex_logging\memex_logging_lib\logging_utils.py` while the file `example.py` contains a usage example

### 0.0.4

* endpoints alignment
* various adjustments
* alpha release @ memex.u-hopper.com

### 0.0.3

* parametrized Elasticsearch instance
* restfull flask introduced
* indexes rename

### 0.0.2

* parametrized parameters -hs -p for the Elasticsearch connection
* docker-support for the deployment
* nginx configuration
* running end-points @ memex.u-hopper.com/logging/LogMessage AND memex.u-hopper.com/logging/LogMessages
* utils class initialized

### 0.0.1

* logging end-points can be tested @ /LogMessage AND /LogMessages
* performance APIs designed
* Elasticsearch connected
