#! /usr/bin/env bash

set -e

#MAVEN_CLI_OPTS="--quiet"
if [[ "$DEBUG" = true ]]
then
  set -x
  env | sort
  MAVEN_CLI_OPTS="${MAVEN_CLI_OPTS} -X"
fi

export MAVEN_CLI_OPTS

# Those check are only useful when you want to build the docker image from your workstation
__check() {
  if [[ "${MVN_GROUP_REPOSITORY}" = "" ]]
  then
    echo "The environment variable MVN_GROUP_REPOSITORY is not set. Set it please."
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
#  MAVEN_CLI_OPTS="${MAVEN_CLI_OPTS} --threads ${NB_THREAD_TO_USE}"

  if [[ "${CI}" = "true" ]]
  then
    git config user.email "${GITLAB_USER_EMAIL}"
    git config user.name "${GITLAB_USER_NAME}"

    MAVEN_CLI_OPTS="${MAVEN_CLI_OPTS} --settings ci/settings.xml
      -Dnexus.user=${NEXUS_USER} \
      -Dnexus.password=${NEXUS_PASSWORD} \
      -Dnexus.group.url=${MVN_GROUP_REPOSITORY} \
      -Dnexus.snapshot.url=${MVN_SNAPSHOT_REPOSITORY} \
      -Dnexus.release.url=${MVN_RELEASE_REPOSITORY}"
  fi

  export MAVEN_CLI_OPTS
}

__code_quality() {
  if [[ "${CI}" = "true" ]]
  then
    if [[ "${SONAR_HOST}" = "" || "${SONAR_TOKEN}" = "" ]]
    then
      echo -e "You need to provide those variables:\n
      - SONAR_HOST
      - SONAR_TOKEN
      "
      exit 1
    fi

    mvn dependency-check:check \
        sonar:sonar \
        -Dsonar.host.url="${SONAR_HOST}" \
        -Dsonar.login="${SONAR_TOKEN}"
  else
    mvn dependency-check:check sonar:sonar
  fi
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

  CURRENT_VERSION=$(mvn help:evaluate -Dexpression=pom.version | egrep -v "^\[")
  echo -e "The current version is: ${CURRENT_VERSION}\n"

  echo "Provide the release name"
  read -r RELEASE_NAME

  echo "Provide the snapshot name"
  read -r SNAPSHOT_NAME

  echo "${SNAPSHOT_NAME}" | grep -i snapshot || SNAPSHOT_NAME="${SNAPSHOT_NAME}-SNAPSHOT"

  if [[ "${CI}" = "true" ]]
  then
    git checkout -B "${CI_BUILD_REF_NAME}"
  fi

  find . -name "pom.xml.releaseBackup" | xargs rm
  test -f release.properties && rm release.properties

  mvn \
    -Dpassword="${GIT_PASSWORD}" \
    -Dusername="${GIT_USER}" \
    -Drelease.name="${RELEASE_NAME}" \
    -Dsnapshot.name="${SNAPSHOT_NAME}" \
    release:prepare
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
      --clean)
        mvn ${MAVEN_CLI_OPTS} clean
        break
        ;;
      --code-quality)
        __code_quality
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
\t- --clean
\t- --code-quality
\t- --deploy-artifact
\t- --install
\t- --package-artifact
\t- --prepare-release
\t- --test
"
   exit 1
fi

