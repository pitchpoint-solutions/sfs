#!/usr/bin/env bash

DOCKER=`/usr/bin/which docker` || { /bin/echo "docker not found in path... $?"; exit 1;}

usage() {
    cat <<EOF
Usage: stop-cluster.sh
EOF
}

${DOCKER} stop $(docker ps -a -q --filter="name=sfs_example")|| { echo "failed to stop containers ${DIRECTORY}. Exiting... $?"; exit 1;}



