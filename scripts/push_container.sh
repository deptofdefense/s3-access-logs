#! /usr/bin/env bash

#
# Build and push docker image to AWS ECR
#

set -eu -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if [ -z "${REPO_NAME}" ]; then
    echo "Must set env var REPO_NAME"
    exit 1
fi

if ! aws --region "${AWS_REGION}" ecr describe-repositories --repository-names="${REPO_NAME}" ; then
    echo "Repository ${REPO_NAME} does not yet exist!"
    exit 1
fi

NAMESPACE="s3-access-logs"
DATE=$(date "+%s")
TAGNAME="${NAMESPACE}-${DATE}"

REPOSITORY_URI=$(aws --region "${AWS_REGION}" ecr describe-repositories --repository-names="${REPO_NAME}" | jq -r .repositories[0].repositoryUri)
LOGIN_URI=$(echo "${REPOSITORY_URI}" | cut -d "/" -f1)

# Log into AWS ECR
aws --region "${AWS_REGION}" ecr get-login-password | docker login --username AWS --password-stdin "${LOGIN_URI}"

# Build, tag and push the image
docker build -f "${DIR}/../Dockerfile" --tag "${NAMESPACE}:latest" --tag "${REPOSITORY_URI}:${TAGNAME}" "${DIR}/.."
docker push "${REPOSITORY_URI}:${TAGNAME}"

REGISTERED_IMAGE=$(docker inspect --format='{{index .RepoDigests 0}}' "${REPOSITORY_URI}:${TAGNAME}")
IMAGE_SHA=$(echo "${REGISTERED_IMAGE}" | cut -d "@" -f2)

echo "*"
echo "New registered image: ${REGISTERED_IMAGE}"
echo "Updating image SHA to: ${IMAGE_SHA}"
echo
