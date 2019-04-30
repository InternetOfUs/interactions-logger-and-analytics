#!/bin/bash

echo "Verifying env variables presence"
declare -a REQUIRED_ENV_VARS=(  "${EL_HOST}",
                                "${EL_PORT}",
                              )

for e in "${REQUIRED_ENV_VARS[@]}"
do
    if [ -z "$e" ]; then
        echo >&2 "Required env variable is missing"
        exit 1
    fi
done

echo "Running ws interfare"
python -m api.logging_apis.main -hs ${EL_HOST} -p ${EL_PORT}
#gunicorn -w 4 -b 0.0.0.0:80 interface:application
