#!/usr/bin/env bash

DOCKER=`/usr/bin/which docker` || { /bin/echo "docker not found in path... $?"; exit 1;}

usage() {
    cat <<EOF
Usage: rm-cluster.sh
EOF
}

${DOCKER} rm $(docker ps -a -q --filter="name=sfs_example")|| { echo "failed to rm containers ${DIRECTORY}. Exiting... $?"; exit 1;}



