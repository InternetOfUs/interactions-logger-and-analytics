#!/bin/bash

DEFAULT_VERSION="0.1.4"

clean () {
    rm -R ${SCRIPT_DIR}/documentation
    rm -R ${SCRIPT_DIR}/src
    rm -R ${SCRIPT_DIR}/migrations
    rm -R ${SCRIPT_DIR}/requirements.txt
}

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PROJECT_DIR=${SCRIPT_DIR}/..

# Identifying build version

VERSION=$1
if [ -z "${VERSION}" ]; then
    VERSION=${DEFAULT_VERSION}
    echo "Build version not specified: building with default version [${VERSION}]"
else
    echo "Building with specified version [${VERSION}]"
fi

# Including required contents

mkdir ${SCRIPT_DIR}/src
cp -R ${PROJECT_DIR}/src/* ${SCRIPT_DIR}/src

mkdir ${SCRIPT_DIR}/documentation
cp -R ${PROJECT_DIR}/documentation/* ${SCRIPT_DIR}/documentation

cp ${PROJECT_DIR}/requirements.txt ${SCRIPT_DIR}

#mkdir ${SCRIPT_DIR}/migrations
#cp -R ${PROJECT_DIR}/migrations/* ${SCRIPT_DIR}/migrations/

# Building image

IMAGE_NAME=memex/logging:${VERSION}
docker build -t ${IMAGE_NAME} ${SCRIPT_DIR}
if [ $? == 0 ]; then

    echo "Build successful: ${IMAGE_NAME}"

    # Tagging images for registry

    echo "Tagging image for push to registry.u-hopper.com:5000"
    docker tag ${IMAGE_NAME} registry.u-hopper.com:5000/${IMAGE_NAME}
    echo "Image can be pushed with:"
    echo "- docker push registry.u-hopper.com:5000/${IMAGE_NAME}"

    # Cleaning
    clean

else
    echo "ERR: Build failed for ${IMAGE_NAME}"

    # Cleaning
    clean
fi

