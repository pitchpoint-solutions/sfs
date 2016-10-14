#!/usr/bin/env bash

DOCKER=`/usr/bin/which docker` || { /bin/echo "docker not found in path... $?"; exit 1;}

usage() {
    cat <<EOF
Usage: start-cluster.sh
EOF
}

${DOCKER} start $(docker ps -a -q --filter="name=sfs_example")|| { echo "failed to start containers. Exiting... $?"; exit 1;}



