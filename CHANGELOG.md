<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Changelog](#changelog)
  - [Breaking changes from 1.* to 2.*](#breaking-changes-from-1-to-2)
  - [Version 2.*](#version-2)
    - [next](#next)
    - [2.1.2](#212)
    - [2.1.1](#211)
    - [2.1.0](#210)
    - [2.0.2](#202)
    - [2.0.1](#201)
    - [2.0.0](#200)
  - [Version 1.*](#version-1)
    - [1.4.0](#140)
    - [1.3.0](#130)
    - [1.2.1](#121)
    - [1.2.0](#120)
    - [1.1.0](#110)
    - [1.0.0](#100)
  - [Version 0.*](#version-0)
    - [0.2.0](#020)
    - [0.1.7](#017)
    - [0.1.6](#016)
    - [0.1.5](#015)
    - [0.1.4](#014)
    - [0.1.3](#013)
    - [0.1.2](#012)
    - [0.1.1](#011)
    - [0.1.0](#010)
    - [0.0.25](#0025)
    - [0.0.24](#0024)
    - [0.0.23](#0023)
    - [0.0.22](#0022)
    - [0.0.21](#0021)
    - [0.0.20](#0020)
    - [0.0.19](#0019)
    - [0.0.18](#0018)
    - [0.0.17](#0017)
    - [0.0.16](#0016)
    - [0.0.15](#0015)
    - [0.0.14](#0014)
    - [0.0.13](#0013)
    - [0.0.12](#0012)
    - [0.0.11](#0011)
    - [0.0.10](#0010)
    - [0.0.9](#009)
    - [0.0.8](#008)
    - [0.0.7](#007)
    - [0.0.6](#006)
    - [0.0.5](#005)
    - [0.0.4](#004)
    - [0.0.3](#003)
    - [0.0.2](#002)
    - [0.0.1](#001)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Changelog

## Breaking changes from 1.* to 2.* 

* the parameter `staticId` of the Analytic has been renamed to `id`
* the parameter `query` of the Analytic has been renamed to `descriptor`
* the key of the _segmentation result_ `count` has been renamed into `segments`
* The following metric values have been renamed 
  - `u:total` into `total`
  - `u:active` into `active`
  - `u:engaged` into `engaged`
  - `u:new` into `new`
  - `a:segmentation` into `age`
  - `g:segmentation` into `gender`
  - `m:from_users` into `requests`
  - `m:segmentation` into `all`
  - `r:segmentation` into `requests`
  - `u:segmentation` into `requests`
  - `m:from_bot` into `from_bot`
  - `m:responses` into `responses`
  - `m:notifications` into `notifications`
  - `t:total` into `total`
  - `t:active` into `active`
  - `t:closed` into `closed`
  - `t:new` into `new`
  - `t:segmentation` into `label`
  - `c:total` into `total`
  - `c:new` into `new`
  - `d:fallback` into `fallback`
  - `d:intents` into `intents`
  - `d:domains` into `domains`
  - `b:response` into `response`
* The keys `items` and `transactions` have been removed from the analytic result contents.

## Version 2.*

### next

- Updated scripts for extracting stats adding also the information whether the users have enabled the survey app.
- Added script for extracting information on app usage by the users.
- Updated to Project template version `4.6.1`.

### 2.1.2

* Updated common models to version `3.1.0` in order to fix the representation errors due to the old Norm model used in the library.

### 2.1.1

* Solved an issue related to integration of endless time window when computing statistics for new users and new conversations.

### 2.1.0

* Added a new `all` analytic window type allowing to analyze all available data up to now.
* The support for periodical (currently the period is set to _once a day, at 4 a.m._) computation of not concluded _fixed time window_ analytic results have been added.
* Changed periodical re-computation of _moving time window_ analytic results execution time to _once a day, at 4 a.m._
* Solved an issue related to aggregations analytics changing aggregations results using always a dictionary.

### 2.0.2

* Message analytics have been updated.
  * Renamed type *from_user* into *requests*
  * Removed analytic of type *from_bot*
* Added two new task metrics: `new_active` and `new_closed`.
* Modified the `closed` task metric to get the number of tasks closed up to the end of a certain time range.
* Removed id of the application from indices of messages and analytics.
* Created an analytic dao for handling the interactions with the database.
* Set aggregation result to `None` if the field where we are performing the aggregation is not present on the message data.
* Added sentry integration in Migrator.

### 2.0.1

* Fixed to broken migration.

### 2.0.0

* Included timestamp details among the analytic result data.
* The support for periodical (currently the period is set to _once a day, at midnight_) re-computation of _moving time window_ analytic results have been added.
* The computation of analytic results is now not triggered automatically upon creation. In order for the results to be computed, it is necessary to wait for the periodical recomputation. It is also possible to take advantage of a new endpoint allowing to explicitly request the result computation.
* _Time window_ type values have been updated. In particular:
  * `DEFAULT` has bene renamed to `moving`
  * `CUSTOM` has been renamed to `fixed`
  Deprecated values are still supported at this stage during the creation step of analytics. Their support will be removed in the upcoming versions.
* An open range of values (previously limited to a small set of options) for days, weeks, months, and years in moving time window is now supported.
* Elasticsearch documents are now stored in the same index they were read from. This was not granted in the previous versions and could cause problems in the data curation process.
* Nullable result values for analytics are now allowed.
* The deletion of an analytics and of the associated results is now supported.
* Added user segmentation analytics for age and gender.
* Re-organised logic for handling analytics:
  * aligned analytic descriptor and result types. Now `count`, `segmentation` and `aggregation` are available ;
  * metrics renamed into more meaningful ones and `c:path`, `c:length`, `m:conversation`, `m:unhandled` are removed;
  * removed the field items from the analytic result.
* Removed deprecated _usercount_ and _eventcount_ endpoints.

## Version 1.*  

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
