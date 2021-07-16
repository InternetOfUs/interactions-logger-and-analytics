#!/bin/bash

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo "Running pre-flight checks..."

SERVICE=$1


if [[ ${SERVICE} == "logger" ]]; then
    echo "Running logger..."
    ${SCRIPT_DIR}/run_logger.sh

elif [[ ${SERVICE} == "worker" ]]; then
    echo "Running worker..."
    ${SCRIPT_DIR}/run_worker.sh

elif [[ ${SERVICE} == "beat" ]]; then
    echo "Running beat..."
    ${SCRIPT_DIR}/run_beat.sh

elif [[ ${SERVICE} == "migrator" ]]; then
    echo "Running migrator..."
    ${SCRIPT_DIR}/run_migrator.sh

else
    echo "Unknown service ${1}"
fi
