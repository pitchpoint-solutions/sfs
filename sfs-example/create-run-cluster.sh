#!/usr/bin/env bash

NC=`/usr/bin/which nc` || { /bin/echo "nc not found in path... $?"; exit 1;}
DOCKER=`/usr/bin/which docker` || { /bin/echo "docker not found in path... $?"; exit 1;}

usage() {
    cat <<EOF
Usage: create-run-cluster.sh -n \${number_of_nodes} -p \${first_port} -d \${working_directory} -h \${docker_host_ip} -r \${number_of_replicas}
EOF
}

function is_port_in_use(){
    ${NC} -z 127.0.0.1 $1;
}

function get_start_container_command(){
    local port; port=${1}
    local directory; directory=${2}
    local clusterHosts; clusterHosts=${3}
    local numberOfReplicas; numberOfReplicas=${4}
    local nodeMaster; nodeMaster=${5}
    local nodeData; nodeData=${6}
    local containerName; containerName=${7}
    local publishAddress; publishAddress=${8}
    local remoteNodeSecret; remoteNodeSecret=${9}
    local keyStoreAwsKmsEndpoint; keyStoreAwsKmsEndpoint=${10}
    local keyStoreAwsKmsKeyId; keyStoreAwsKmsKeyId=${11}
    local keyStoreAwsKmsAccessKeyId; keyStoreAwsKmsAccessKeyId=${12}
    local keyStoreAwsKmsSecretKey; keyStoreAwsKmsSecretKey=${13}
    local keyStoreAzureKmsEndpoint; keyStoreAzureKmsEndpoint=${14}
    local keyStoreAzureKmsKeyId; keyStoreAzureKmsKeyId=${15}
    local keyStoreAzureKmsAccessKeyId; keyStoreAzureKmsAccessKeyId=${16}
    local keyStoreAzureKmsSecretKey; keyStoreAzureKmsSecretKey=${17}
    local elasticsearchClusterName; elasticsearchClusterName=${18}
    local elasticsearchNodeName; elasticsearchNodeName=${19}
    local elasticsearchHosts; elasticsearchHosts=${20}
    local commandToExecute; commandToExecute="${DOCKER} run -it \
    --add-host localhost:127.0.0.1 \
    -e INSTANCES=200 \
    -e HEAP_SIZE=512m \
    -e SFS_FS_HOME=/data/sfs \
    -e SFS_CLUSTER_HOSTS=${clusterHosts} \
    -e SFS_NUMBER_OF_OBJECT_REPLICAS=${numberOfReplicas} \
    -e SFS_NODE_MASTER=${nodeMaster} \
    -e SFS_NODE_DATA=${nodeData} \
    -e SFS_HTTP_LISTEN_ADDRESSES=0.0.0.0:80 \
    -e SFS_HTTP_PUBLISH_ADDRESSES=${publishAddress} \
    -e SFS_REMOTENODE_SECRET=${remoteNodeSecret} \
    -e SFS_KEYSTORE_AWS_KMS_ENDPOINT=${keyStoreAwsKmsEndpoint} \
    -e SFS_KEYSTORE_AWS_KMS_KEY_ID=${keyStoreAwsKmsKeyId} \
    -e SFS_KEYSTORE_AWS_KMS_ACCESS_KEY_ID=${keyStoreAwsKmsAccessKeyId} \
    -e SFS_KEYSTORE_AWS_KMS_SECRET_KEY=${keyStoreAwsKmsSecretKey} \
    -e SFS_KEYSTORE_AZURE_KMS_ENDPOINT=${keyStoreAzureKmsEndpoint} \
    -e SFS_KEYSTORE_AZURE_KMS_KEY_ID=${keyStoreAzureKmsKeyId} \
    -e SFS_KEYSTORE_AZURE_KMS_ACCESS_KEY_ID=${keyStoreAzureKmsKeyId} \
    -e SFS_KEYSTORE_AZURE_KMS_SECRET_KEY=${keyStoreAzureKmsSecretKey} \
    -e SFS_ELASTICSEARCH_CLUSTER_NAME=${elasticsearchClusterName} \
    -e SFS_ELASTICSEARCH_NODE_NAME=${elasticsearchNodeName} \
    -e SFS_ELASTICSEARCH_DISCOVERY_ZEN_PING_UNICAST_HOSTS=${elasticsearchHosts} \
    -e SFS_ELASTICSEARCH_DISCOVERY_ZEN_PING_MULTICAST_ENABLED=false \
    -e SFS_ELASTICSEARCH_DISCOVERY_ZEN_PING_UNICAST_ENABLED=true \
    --detach \
    --name ${containerName} \
    -p ${port}:80 \
    simple-file-server:0-SNAPSHOT"
    echo ${commandToExecute}
}

function get_working_directory_for_port(){
    local directory; directory=$1
    local port; port=$2
    echo "${WORKING_DIRECTORY}/sfs_example_node_${port}"
}

function get_container_name(){
    local port; port=$1
    echo "sfs_example_cluster_${port}"
}

function checkArgumentIsSet(){
    local variableName; variableName=$1
    local variableValue; variableValue=$2
    if [ "$variableValue" == "" ]; then
        echo "${variableName} is not set"
        exit 1
    fi
}

NUMBER_OF_NODES=1
NUMBER_OF_REPLICAS=0
FIRST_PORT=8080
PUBLISH_IP=""
WORKING_DIRECTORY=""
while getopts n:p:d:h:r: opt; do
   case "$opt" in
      n) NUMBER_OF_NODES=$OPTARG;;
      p) FIRST_PORT=$OPTARG;;
      d) WORKING_DIRECTORY=$OPTARG;;
      h) PUBLISH_IP=$OPTARG;;
      r) NUMBER_OF_REPLICAS=$OPTARG;;
      ?) usage;;
   esac
done


if [[ "$NUMBER_OF_NODES" -lt 1 ]]; then
  usage
  exit 1
fi

if [[ "$FIRST_PORT" -lt 1 ]]; then
  usage
  exit 1
fi

if [[ "$NUMBER_OF_REPLICAS" -lt 0 ]]; then
  usage
  exit 1
fi

if [ "$WORKING_DIRECTORY" == "" ]; then
  usage
  exit 1
fi

if [ -f ${WORKING_DIRECTORY}/conf/sfsenv.sh ]
then
 . ${WORKING_DIRECTORY}/conf/sfsenv.sh
fi


PUBLISH_IP=${PUBLISH_IP:-"127.0.0.1"}
NUMBER_OF_REPLICAS=${NUMBER_OF_REPLICAS:-"0"}

LAST_PORT=$(($FIRST_PORT + $NUMBER_OF_NODES - 1))
ELASTICSEARCH_PORT=$((LAST_PORT + 1))
ELASTICSEARCH_CLUSTER_NAME="sfs_example_cluster"

checkArgumentIsSet "SFS_KEYSTORE_AWS_KMS_ENDPOINT" ${SFS_KEYSTORE_AWS_KMS_ENDPOINT}
checkArgumentIsSet "SFS_KEYSTORE_AWS_KMS_KEY_ID" ${SFS_KEYSTORE_AWS_KMS_KEY_ID}
checkArgumentIsSet "SFS_KEYSTORE_AWS_KMS_ACCESS_KEY_ID" ${SFS_KEYSTORE_AWS_KMS_ACCESS_KEY_ID}
checkArgumentIsSet "SFS_KEYSTORE_AWS_KMS_SECRET_KEY" $SFS_KEYSTORE_AWS_KMS_SECRET_KEY}

checkArgumentIsSet "SFS_KEYSTORE_AZURE_KMS_ENDPOINT" ${SFS_KEYSTORE_AZURE_KMS_ENDPOINT}
checkArgumentIsSet "SFS_KEYSTORE_AZURE_KMS_KEY_ID" ${SFS_KEYSTORE_AZURE_KMS_KEY_ID}
checkArgumentIsSet "SFS_KEYSTORE_AZURE_KMS_ACCESS_KEY_ID" ${SFS_KEYSTORE_AZURE_KMS_ACCESS_KEY_ID}
checkArgumentIsSet "SFS_KEYSTORE_AZURE_KMS_SECRET_KEY" $SFS_KEYSTORE_AZURE_KMS_SECRET_KEY}

if is_port_in_use ${ELASTICSEARCH_PORT}; then
    echo "Elasticsearch port ${ELASTICSEARCH_PORT} is in use. Exiting..."; exit 1;
fi

declare -a CLUSTER_HOSTS
for port in `seq ${FIRST_PORT} ${LAST_PORT}`; do
  if is_port_in_use ${port}; then
    echo "Port ${port} is in use. Exiting..."; exit 1;
  else
    CLUSTER_HOSTS+=("${PUBLISH_IP}:${port}")
  fi
done
CLUSTER_HOSTS_AS_STRING=`printf "%s," "${CLUSTER_HOSTS[@]}" | cut -d "," -f 1-${#CLUSTER_HOSTS[@]}`


ELASTICSEARCH_HOME="${WORKING_DIRECTORY}/sfs_example_elasticsearch"

START_ELASTICSEARCH_COMMAND="${DOCKER} run -d -p ${ELASTICSEARCH_PORT}:9300 --name sfs_example_elasticsearch elasticsearch:2.4.1 -Des.cluster.name=${ELASTICSEARCH_CLUSTER_NAME}"
echo "Starting elasticsearch docker container ${START_ELASTICSEARCH_COMMAND}."
${START_ELASTICSEARCH_COMMAND} || { echo "failed to start elasticsearch in ${ELASTICSEARCH_HOME}. Exiting... $?"; exit 1;}

NODE_MASTER=true
declare -a START_CONTAINER_COMMANDS
for port in `seq ${FIRST_PORT} ${LAST_PORT}`; do
    DIRECTORY=$(get_working_directory_for_port ${WORKING_DIRECTORY} ${port})
    CONTAINER_NAME=$(get_container_name ${port})
    PUBLISH_ADDRESS="${PUBLISH_IP}:${port}"
    ELASTICSEARCH_ADDRESS="${PUBLISH_IP}:${ELASTICSEARCH_PORT}"
    START_CONTAINER_COMMAND=$(get_start_container_command \
    ${port} \
    ${DIRECTORY} \
    ${CLUSTER_HOSTS_AS_STRING} \
    ${NUMBER_OF_REPLICAS} \
    ${NODE_MASTER} \
    "true" \
    ${CONTAINER_NAME} \
    ${PUBLISH_ADDRESS} \
    "YWJjMTIzCg==" \
    ${SFS_KEYSTORE_AWS_KMS_ENDPOINT} \
    ${SFS_KEYSTORE_AWS_KMS_KEY_ID} \
    ${SFS_KEYSTORE_AWS_KMS_ACCESS_KEY_ID} \
    ${SFS_KEYSTORE_AWS_KMS_SECRET_KEY} \
    ${SFS_KEYSTORE_AZURE_KMS_ENDPOINT} \
    ${SFS_KEYSTORE_AZURE_KMS_KEY_ID} \
    ${SFS_KEYSTORE_AZURE_KMS_ACCESS_KEY_ID} \
    ${SFS_KEYSTORE_AZURE_KMS_SECRET_KEY} \
    ${ELASTICSEARCH_CLUSTER_NAME} \
    "sfs_example_node_${port}" \
    ${ELASTICSEARCH_ADDRESS})
    NODE_MASTER=false
    echo "Starting sfs docker container ${START_CONTAINER_COMMAND}"
    ${START_CONTAINER_COMMAND}
done


