#! /usr/bin/env bash

set -e

MAVEN_CLI_OPTS="-B"
if [[ "$DEBUG" = true ]]
then
  set -x
  env | sort
  MAVEN_CLI_OPTS="${MAVEN_CLI_OPTS} -X"
fi

export MAVEN_CLI_OPTS

# Those check are only useful when you want to build the docker image from your workstation
__check() {
  if [[ "${DOCKER_REGISTRY}" = "" ]]
  then
    echo "The environment variable DOCKER_REGISTRY is not set. Set it please."
    exit 1
  fi

  if [[ "${MVN_RELEASE_REPOSITORY}" = "" ]]
  then
    echo "The environment variable MVN_RELEASE_REPOSITORY is not set. Set it please."
    exit 1
  fi

  if [[ "${MVN_SNAPSHOT_REPOSITORY}" = "" ]]
  then
    echo "The environment variable MVN_SNAPSHOT_REPOSITORY is not set. Set it please."
    exit 1
  fi

  if [[ "${NEXUS_USER}" = "" ]]
  then
    echo "The environment variable NEXUS_USER is not set. Set it please."
    exit 1
  fi

  if [[ "${NEXUS_PASSWORD}" = "" ]]
  then
    echo "The environment variable NEXUS_PASSWORD is not set. Set it please."
    exit 1
  fi

  if [[ "${CI_COMMIT_SHA}" = "" ]]
  then
    CI_COMMIT_SHA=$(git rev-parse HEAD)
    export CI_COMMIT_SHA
  fi

  if [[ "${CI_COMMIT_BRANCH}" = "" ]]
  then
    if ! [[ "${CI_COMMIT_REF_NAME}" = "" ]]
    then
      CI_COMMIT_BRANCH=${CI_COMMIT_REF_NAME}
    else
      CI_COMMIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    fi

    export CI_COMMIT_BRANCH
  fi
}

__init() {
  SHORT_COMMIT_ID=$(echo "${CI_COMMIT_SHA}" | cut -c 1-7)
  export SHORT_COMMIT_ID

  DOCKER_IMAGE_BASE=$(echo "${DOCKER_REGISTRY}" | sed "s/\/$//")
  export DOCKER_IMAGE_BASE
  if ! [[ "${CI_COMMIT_TAG}" = "" ]]
  then
    export TYPE=release
    export ID=${CI_COMMIT_TAG}
  else
    export TYPE=snapshot
    export ID=${CI_COMMIT_BRANCH}-${SHORT_COMMIT_ID}
  fi

  # Count the number of thread to use for maven
  OS_TYPE=$(uname -s)
  case ${OS_TYPE} in
    Darwin)
      NB_THREAD_TO_USE=$(sysctl -n hw.ncpu)
      ;;
    Linux)
      NB_THREAD_TO_USE=$(grep -i -c "^processor" /proc/cpuinfo)
      ;;
    *)
      ;;
  esac
  NB_THREAD_TO_USE=$((NB_THREAD_TO_USE / 2))
  export NB_THREAD_TO_USE

  DOCKER_IMAGE_NAME=$(mvn help:evaluate \
    -B \
    -Dexpression=project.artifactId \
    -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn | \
    grep -v -E "^\[")
  export DOCKER_IMAGE_NAME

  DOCKER_IMAGE_ID=${DOCKER_IMAGE_BASE}/${DOCKER_IMAGE_NAME}:${TYPE}--${ID}
  export DOCKER_IMAGE_ID

  if [[ "${CI_COMMIT_TAG}" = "" ]]
  then
    DOCKER_LATEST_IMAGE_ID=${DOCKER_IMAGE_BASE}/${DOCKER_IMAGE_NAME}:${TYPE}--${CI_COMMIT_BRANCH}--latest
  else
    DOCKER_LATEST_IMAGE_ID=${DOCKER_IMAGE_BASE}/${DOCKER_IMAGE_NAME}:${TYPE}--latest
  fi
  export DOCKER_LATEST_IMAGE_ID

  if [[ "${CI}" = "true" ]]
  then
    git config user.email "${GITLAB_USER_EMAIL}"
    git config user.name "${GITLAB_USER_NAME}"

    MAVEN_CLI_OPTS="${MAVEN_CLI_OPTS} --settings ci/settings.xml
      -Dnexus.user=${NEXUS_USER} \
      -Dnexus.password=${NEXUS_PASSWORD} \
      -Dnexus.snapshot.url=${MVN_SNAPSHOT_REPOSITORY} \
      -Dnexus.release.url=${MVN_RELEASE_REPOSITORY}"
  fi
  MAVEN_CLI_OPTS="${MAVEN_CLI_OPTS} --threads ${NB_THREAD_TO_USE}"
  export MAVEN_CLI_OPTS
}

__login_docker() {
  echo "${NEXUS_PASSWORD}" | docker login --username "${NEXUS_USER}" --password-stdin "${DOCKER_REGISTRY}"
}

__build_docker() {
  docker build -t "${DOCKER_IMAGE_ID}" .
}

__prepare_release() {
  if [[ -z "${CI_PROJECT_PATH}" || -z "${GIT_USER}" || -z "${GIT_PASSWORD}" ]]
  then
    echo -e "These environment variables must be set: \n \
    - CI_PROJECT_PATH \n \
    - GIT_USER \n \
    - GIT_PASSWORD"
    exit 1
  fi

  RELEASE_NAME=$(cicd_release \
    --gitlab-server "${CI_SERVER_URL}" \
    --gitlab-token "${GIT_PASSWORD}" \
    --full-path-project "${CI_PROJECT_PATH}" \
    --generate-release-name | \
    sed -re 's/^.*: (.*)$/\1/')

  SNAPSHOT_NAME=$(cicd_release \
    --gitlab-server "${CI_SERVER_URL}" \
    --gitlab-token "${GIT_PASSWORD}" \
    --full-path-project "${CI_PROJECT_PATH}" \
    --generate-snapshot-name | \
    sed -re 's/^.*: (.*)$/\1/')

  if [[ "${CI}" = "true" ]]
  then
    git checkout -B "${CI_BUILD_REF_NAME}"
  fi

  mvn -B \
    -Dpassword="${GIT_PASSWORD}" \
    -Dusername="${GIT_USER}" \
    -Drelease.name="${RELEASE_NAME}" \
    -Dsnapshot.name="${SNAPSHOT_NAME}" \
    release:prepare
}

__push_docker() {
  docker tag "${DOCKER_IMAGE_ID}" "${DOCKER_LATEST_IMAGE_ID}"
  docker push "${DOCKER_LATEST_IMAGE_ID}"
  docker rmi "${DOCKER_LATEST_IMAGE_ID}"
  echo "Docker image successfully pushed: ${DOCKER_LATEST_IMAGE_ID}"

  docker push "${DOCKER_IMAGE_ID}"
  docker rmi "${DOCKER_IMAGE_ID}"
  echo "Docker image successfully pushed: ${DOCKER_IMAGE_ID}"
}

# Main
__check
__init

if ! [[ $# -eq 0 ]]
then
  ARGUMENT=$1
  while [[ -n ${ARGUMENT} ]];
  do
    case "${ARGUMENT}" in
      --build-docker)
        __login_docker
        __build_docker
        break
        ;;
      --clean)
        mvn ${MAVEN_CLI_OPTS} clean
        break
        ;;
      --deploy-artifact)
        mvn ${MAVEN_CLI_OPTS} -DskipTests deploy
        break
        ;;
      --install)
        mvn ${MAVEN_CLI_OPTS} -DskipTests install
        break
        ;;
      --package-artifact)
        mvn ${MAVEN_CLI_OPTS} -DskipTests package
        break
        ;;
      --prepare-release)
        __prepare_release
        break
        ;;
      --push-docker)
        __login_docker
        __push_docker
        break
        ;;
      --test)
        mvn ${MAVEN_CLI_OPTS} test
        break
        ;;
      *)
        echo "Unknown option: ${OPTION}"
        exit 1
        ;;
    esac
  done
else
  echo -e "Provided one of these options to the script $0:\n\
\t- --build-docker
\t- --clean
\t- --deploy-artifact
\t- --install
\t- --package-artifact
\t- --prepare-release
\t- --push-docker
\t- --test
"
   exit 1
fi

