# Changelog

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [0.* Versions](#0-versions)
  - [0.2.* Versions](#02-versions)
    - [0.2.0](#020)
  - [0.1.* Versions](#01-versions)
    - [0.1.7](#017)
    - [0.1.6](#016)
    - [0.1.5](#015)
    - [0.1.4](#014)
    - [0.1.3](#013)
    - [0.1.2](#012)
    - [0.1.1](#011)
    - [0.1.0](#010)
  - [0.0.* Versions](#00-versions)
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

## 0.* Versions

### 0.2.* Versions

#### 0.2.0

- added apache licence specification

### 0.1.* Versions

#### 0.1.7

- added aggregation

#### 0.1.6

- analytics and documentation improvements

#### 0.1.5

- indices of logs and messages improvements

#### 0.1.4

- documentation is up to date

#### 0.1.3

- documentation is up to date

#### 0.1.2

- click counters available

#### 0.1.1

- notification works

#### 0.1.0

- analytics, logging and documentation improvements
- MultiAction responses are supported

### 0.0.* Versions

#### 0.0.25

- carousel documentation updated

#### 0.0.24

- documentation updated

#### 0.0.23

- entities bug in request messages solved

#### 0.0.22

- logs are now working
- the library support logs and notifications
- the models up-to date
- documentation up-to date

#### 0.0.21

- new log data model
- new log APIs and adjustments to the CarouselResponse model
- documentation up-to date

#### 0.0.20

- date parsing is working

#### 0.0.19

- new approach to retrieve the messages
- location is now supported
- new approach to the timestamp management

#### 0.0.18

- structure_id should be in the metadata
- timestamp is again an ISO text data

#### 0.0.17

- small adjustments in the message logging APIs

#### 0.0.16

- library update, data model update and utils update

#### 0.0.15

- updated version tag in the build.sh file of the docker-support

#### 0.0.14

- entities optional

#### 0.0.13

- intent and domain are optional parameters

#### 0.0.12

- bug fixes
- data model - Notification

#### 0.0.11

- updated version tag in the build.sh file of the docker-support

#### 0.0.10

- fixed documentation endpoint
- updated logging library, MPS1726

#### 0.0.9

- updated logging library

#### 0.0.8

- documentation folder loaded by build.sh

#### 0.0.7

- new library with better examples + bug corrected in messaging APIs

#### 0.0.6

- new data models for messages
- new data models for logging
- alpha version data model of analytics
- end-point for the documentation
- new version of the messages APIs

#### 0.0.5

- minor adjustments
- default value of the project is memex. Whenever a message without the project value arrives, it is assigned to memex automatically
- utility package to use the APIs is now part of the release. The utilities can be found in `memex_logging\memex_logging_lib\logging_utils.py` while the file `example.py` contains a usage example

#### 0.0.4

- endpoints alignment
- various adjustments
- alpha release @ memex.u-hopper.com

#### 0.0.3

- parametrized Elasticsearch instance
- restfull flask introduced
- indexes rename

#### 0.0.2

- parametrized parameters -hs -p for the Elasticsearch connection
- docker-support for the deployment
- nginx configuration
- running end-points @ memex.u-hopper.com/logging/LogMessage AND memex.u-hopper.com/logging/LogMessages
- utils class initialized

#### 0.0.1

- logging end-points can be tested @ /LogMessage AND /LogMessages
- performance APIs designed
- Elasticsearch connected
