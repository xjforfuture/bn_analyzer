#!/bin/sh

PARAM1=${1}
PARAM2=${2}

PROJECT_PATH=$(cd `dirname $0`; pwd)
PROJECT_NAME="${PROJECT_PATH##*/}"

VERSION=release

VER_GEN_CMD=./verinfo.sh
VER_INFO_NAME=docker/version.info

CLEAN=0

rm -f ${VER_INFO_NAME}

if [ ${PARAM1} -a ${PARAM1} = "base" ]; then
    TAG_PREFIX=cdqhyx.cn:8888/base/${PROJECT_NAME}
    BUILD_CONTEXT=./
    DOCKERFILE=./docker/Dockerfile-base

    if [ ${PARAM2} -a ${PARAM2} = "clean" ]; then
        CLEAN=1
    fi
else
    TAG_PREFIX=cdqhyx.cn:8888/tianshu/${PROJECT_NAME}
    BUILD_CONTEXT=./
    cp .dockerignore ${BUILD_CONTEXT}
    DOCKERFILE=./docker/Dockerfile

    if [ ${PARAM1} -a ${PARAM1} = "clean" ]; then
        CLEAN=1
    fi
fi

TAG=${TAG_PREFIX}:${VERSION}

if test ${CLEAN} -ne 0; then
    docker rmi ${TAG} --force
    exit 0
fi

echo "##### start building docker images ${TAG} ..."
docker build -t ${TAG} ${BUILD_CONTEXT} -f ${DOCKERFILE}