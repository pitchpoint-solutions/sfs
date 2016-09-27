# Simple File Server

[![Build Status]

## Overview
* Sfs aims to be a file server that can serve and securely store billions of large and small files using minimal resources. 
* The http api implements common features of the openstack swift http api so that it can be used with exiting tools.


## Features
* Object metadata is indexed in elasticsearch which allows for quick reads, updates, deletes and listings even when containers container millions of objects
* Objects are versioned and the number of revisions stored can be configured per container. This means that you'll never loose by object by overwriting it or deleting it (unless you force a deletion).
* Each object can have a TTL set on it so that manual purging is not required
* Object data is stored in data files that are themselves replicated and healed when necessary (the object data replication level can be controlled by container independently of the object index settings). If the object size is very small it's stored with the object metadata instead of the data file. This is useful if you're storing token type data.
* Object data files do not need to be compacted since the block ranges are recycled. The range allocator attempts to be intelligent about which ranges object data is written to so that writes are sequential.
* Maximum object size is 5GB. It's defined at build time and can be changed.
* Objects of many terabytes are supported through the openstack swift dynamic large object functionality
* Each container gets it's own index so that object metadata sharding and replication can be controlled on a container level. 
* Object data is encrypted at rest using AES256-GCM if the container is configured to encrypt by default or the object upload request includes the "X-Server-Side-Encryption" http header
* Master keys are automatically generated, rotated and stored on redundant key management services (Amazon KMS and Azure KMS). You will need accounts on both services but since sfs uses a tiny amount of master keys the charges are minimal.
* Container encryption keys are automatically generated, rotated and not stored in plain text anywhere. Once sfs starts it initializes the master keys and when a container key needs to be decrypted it uses the appropriate master key.
* A container can be exported into into a file and imported into another container. The dynamic large object manifest will also be updated if it references objects in the container that was exported. The container export format is independent of the index and data file format so that an export can be imported into any sfs version that supports the export file format.
* Container exports can be compressed and encrypted using AES256-GCM if the appropriate http headers are supplied to the http export api
* Adding new sfs nodes to the cluster is a simple as starting the docker image on another server. New data will always be written to the nodes with the most available storage space. Existing data will not be rebalanced.  
* The entire implementation is event driven and non blocking. Built using vert.x.


## Master Keys
* On first use 1 master is key is generated if one does not exist
* A new master key is generated every 30 days 
* Master keys are re-encrypted every 30 days
* The primary copy of the master key is encrypted using amazons kms
* The backup copy of the master key is encrypted using azures kms
* If the primary copy of the master key can't be decrypted the backup copy will be used repair the primary copy
* If the backup copy of the master key can't be decrypted the primary copy will be used to repair the backup copy
* When a master key is needed it is decrypted using the kms and stored encrypted locally in memory so that every use of the master key doesn't require a call to the kms. This means after a restart each master key that is used will only call the kms to decrypt the key once.


## Container Keys
* When a container key is needed a new one is generated if none exist.
* If the container key been active for more than 30 days and new one is generated. 
* The container key is encrypted using AES256-GCM w/ nonce using the latest master key and stored encrypted as part of the container metadata.
* When object data needs to be encrypted the latest container key is fetched, decrypted using the appropriate master key.
* When object data needs to be decrypted the appropriate container key is fetched, decrypted using the appropriate master key.
* Container keys are re-encrypted every 30 days


## Object Encryption
* Each object is encrypted using AES256-GCM w/ nonce


## Metadata Replication
* Metadata replication uses the elasticsearch settings


## Object Data Replication
* Object streams are replicated in real time. This means that if you upload a 5GB file you don't have to wait for the data to be copied from the primary volume to the replicas after the write to the primary has finished.
* This is implemented by cloning the data stream to each volume as sfs recives the stream and then validating checksums before an OK response is returned to the user.


## Backups
* Backups support is provided by container export/import functionality
* The container export stream can be compressed and encrypted so that it doesn't have to be done afterwards. For large containers this saves many hours of time.
* The export storage format is independent of the index and volume format so that major version upgrades of elasticsearch and sfs can be executed without fear of data loss. 


## Volume Block Allocation
* When and object uploaded is one continuous block range is allocated in the volume


## Access Control
* The default auth provider is org.sfs.auth.SimpleAuthProvider and is intentionally very simple since sfs will likely be hosted behind some form of proxy the controls access or an application that delegates secure storage to sfs. In this auth provider if credentials match a user role then the user can PUT, POST, HEAD, GET, DELETE containers and objects and GET accounts. The user role will only be allowed to see objects and containers that they're allowed to read when listing accounts and containers (in the case of SimpleAuthProvider it's everything). If credentials match an admin role everything is allowed. See "Adding a new AuthProvider" later in this readme.
* The auth provider interface provides support for create, update, delete and read operations on accounts, containers, and objects.

## Adding a new AuthProvide
1. Create a new project or module and include sfs-server as a dependency (my-organization-sfs-server). 
2. Implement org.sfs.auth.AuthProvider and make priority() return a value higher than 0
3. Create a file named my-organization-sfs-server/src/main/resources/META-INF/services/org.sfs.auth.AuthProvider
4. Put the name of your implementation class into the my-organization-sfs-server/src/main/resources/META-INF/services/org.sfs.auth.AuthProvider file
5. Using sfs-docker/main/docker/Dockerfile as an example create a new artifact (my-organization-sfs-docker) that uses your new server artifact (my-organization-sfs-server) as a dependency instead of sfs-server


## Caveats
* You will need to run an elasticsearch cluster
* Each data node hosts a volume (this is where object data is stored). If you set the object replica count (x-sfs-object-replicas) higher than the number of nodes+1 object uploads will fail since the replicated data needs to be stored on different nodes. org.sfs.nodes.Nodes.allowSameNode allows this behaviour to be overridden but currently allowSameNode is used only for testing. If instances are being run using docker containers then this is not an issue since each instance gets it's own file system and thus it's own node id. 
* The x-sfs-object-replicas value is dynamic and can be incremented while the system is running when more nodes come online. 


## Building

Oracle Java 8 and Maven 3 should be used for building.

###### Environment Variables ######
    SFS_KEYSTORE_AWS_KMS_ENDPOINT=https://kms.us-east-1.amazonaws.com
    SFS_KEYSTORE_AWS_KMS_KEY_ID=...
    SFS_KEYSTORE_AWS_KMS_ACCESS_KEY_ID=$...
    SFS_KEYSTORE_AWS_KMS_SECRET_KEY=...
    
    SFS_KEYSTORE_AZURE_KMS_ENDPOINT=https://....vault.azure.net
    SFS_KEYSTORE_AZURE_KMS_KEY_ID=...
    SFS_KEYSTORE_AZURE_KMS_ACCESS_KEY_ID=...
    SFS_KEYSTORE_AZURE_KMS_SECRET_KEY=...

###### Building, testing and assembling Docker image (from the root project directory) ######
    mvn clean package
    
###### Building, testing (from the sfs-server directory) ######
    mvn clean package    
    
###### Building, testing and regenerating the protobuf files (from the sfs-server directory) ######
    mvn clean package -Pprotoc     
    
    
## Running (Requires Elasticsearch 2.4)    

* [Run a test elasticsearch instance](https://mad.is/2016/09/running-elasticsearch-in-docker-container/)
* [Run a production elasticsearch cluster](https://www.elastic.co/guide/en/elasticsearch/guide/current/deploy.html)

###### Running the docker image you just built (See sample configuration and logback configuration) ######
    docker run -it --add-host "es-host:${HOST_IP}" --add-host "localhost:127.0.0.1" -e "INSTANCES=200" -e "HEAP_SIZE=512m" -p 8092:8092 -v ${PWD}:/data -v ${PWD}/sample-config.json:/etc/vertx-conf.json -v ${PWD}/sample-logback.xml:/etc/vertx-logback.xml pps-sfs:latest

###### Sample Configuration ######
    {
      "fs.home": "/data/sfs",
      "node.data": true,
      "node.master": true,
      "number_of_replicas": 0,
      "http.listen.addresses": "0.0.0.0",
      "http.publish.addresses": "${docker_image_ip_address}"
      "http.listen.port": 8092,
      "http.maxheadersize": 40960,
      "jobs.maintenance_interval": "86400000",
      "remotenode.maxpoolsize": 200,
      "remotenode.connectimeout": 5000,
      "remotenode.responsetimeout": 10000,
      "remotenode.secret": "YWJjMTIzCg==",
      "elasticsearch.cluster.name": "elasticsearch_samplecluster",
      "elasticsearch.node.name": "simple-file-server-client",
      "elasticsearch.replicas": "0",
      "elasticsearch.shards": "12",
      "elasticsearch.discovery.zen.ping.multicast.enabled": false,
      "elasticsearch.discovery.zen.ping.unicast.enabled": true,
      "elasticsearch.discovery.zen.ping.unicast.hosts": [
        "es-host:9300"
      ],
      "elasticsearch.defaultsearchtimeout": 30000,
      "elasticsearch.defaultindextimeout": 30000,
      "elasticsearch.defaultgettimeout": 30000,
      "elasticsearch.defaultdeletetimeout": 30000,
      "keystore.aws.kms.endpoint": "https://kms.us-east-1.amazonaws.com",
      "keystore.aws.kms.key_id": "${aws_kms_key_id}",
      "keystore.aws.kms.access_key_id": "${aws_kms_access_key_id}",
      "keystore.aws.kms.secret_key": "${aws_kms_secret_key}",
      "keystore.azure.kms.endpoint": "https://....vault.azure.net",
      "keystore.azure.kms.key_id": "${aws_azure_key_id}",
      "keystore.azure.kms.access_key_id": "${aws_azure_access_key_id}",
      "keystore.azure.kms.secret_key": "${aws_azure_secret_key}",
      "auth": {
        "admin": [
          {
            "username": "admin",
            "password": "admin"
          }
        ],
        "user": [
          {
            "username": "user",
            "password": "user"
          }
        ]
      }
    }
    
###### Sample Logback Configuration ######
    <?xml version="1.0" encoding="UTF-8" ?>
    
    <configuration scan="true">
    
        <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
            <!-- encoders are assigned by default the type
                 ch.qos.logback.classic.encoder.PatternLayoutEncoder -->
            <encoder>
                <pattern>%d [%thread] %-5level %logger{36} - %msg%n</pattern>
            </encoder>
        </appender>
    
        <logger name="org.apache" level="TRACE" additivity="false">
            <appender-ref ref="STDOUT"/>
        </logger> d
    
        <logger name="io.vertx" level="TRACE" additivity="false">
            <appender-ref ref="STDOUT"/>
        </logger>
    
        <logger name="io.netty" level="DEBUG" additivity="false">
            <appender-ref ref="STDOUT"/>
        </logger>
    
        <logger name="pps" level="INFO" additivity="false">
            <appender-ref ref="STDOUT"/>
        </logger>
    
    </configuration>


## Accounts

###### Create/Update Account ######
    curl -XPOST -u admin:admin "http://localhost:8092/openstackswift001/my_account"
###### Get Account Metadata ######
    curl -XHEAD -u admin:admin "http://localhost:8092/openstackswift001/my_account"
###### List Containers in Account ######
    curl -XGET -u admin:admin "http://localhost:8092/openstackswift001/my_account"
###### Delete Account (if it's empty) ######
    curl -XDELETE -u admin:admin "http://localhost:8092/openstackswift001/my_account"    



## Containers

###### Create a Container ######
    curl -XPUT -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container"
###### Update a Container ######
    curl -XPOST -u admin:admin -H "X-Container-Meta-Server-Side-Encryption: true" -H "X-Container-Meta-Max-Object-Revisions: 3" "http://localhost:8092/openstackswift001/my_account/my_container"    
###### Get Container Metadata ######
    curl -XHEAD -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container"
###### List Objects in Container ######
     curl -XGET -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container?format=xml&prefix=&limit=10000&delimiter=%2F"
###### List Objects in subfolder ######
    curl -XGET -u admin:admin "http://localhost:8092/sfs/openstackswift001/my_account/my_container?format=xml&prefix=subfolder%2F&limit=10000&delimiter=%2F
###### Create a Container the by default encrypts objects and retains at most 3 object revisions ######
    curl -XPUT -u admin:admin -H "X-Container-Meta-Server-Side-Encryption: true" -H "X-Container-Meta-Max-Object-Revisions: 3" "http://localhost:8092/openstackswift001/my_account/my_container"
###### Create a Container and control the number of object index shards, the number of object index replicas and the number of object replicas ######
    curl -XPUT -u admin:admin -H "x-sfs-object-index-shards: 12" -H "x-sfs-object-index-replicas: 2" -H "x-sfs-object-replicas: 2" "http://localhost:8092/openstackswift001/my_account/my_container"
###### Update a Container so that by default it encrypts objects and retains at most 2 object revisions ######
    curl -XPOST -u admin:admin -H "X-Container-Meta-Server-Side-Encryption: true" -H "X-Container-Meta-Max-Object-Revisions: 2" "http://localhost:8092/openstackswift001/my_account/my_container"
###### Update a Container so that by default it doesn't encrypt objects and retains at most 1 object revisions ######
    curl -XPOST -u admin:admin -H "X-Container-Meta-Server-Side-Encryption: false" -H "X-Container-Meta-Max-Object-Revisions: 1" "http://localhost:8092/openstackswift001/my_account/my_container"
###### Update a Container so that one object index replica in maintained for each object index shard and one each object replicated once ######
    curl -XPOST -u admin:admin -H "x-sfs-object-index-replicas: 1" -H "x-sfs-object-replicas: 1" "http://localhost:8092/openstackswift001/my_account/my_container"
###### Delete a Container (if it's empty) ######
    curl -XDELETE -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container"  
    
    
## Object Storage 
   
###### Upload and object ######
    curl -XPUT -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container/my_object" -d 'abc123'
###### Upload and object and have it be stored encrypted ######
    curl -XPUT -u admin:admin -H "X-Server-Side-Encryption: true" "http://localhost:8092/openstackswift001/my_account/my_container/my_object" -d 'abc123'
###### Get object metadata ######
    curl -XHEAD -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container/my_object"
###### Get object ######
    curl -XGET -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container/my_object" 
###### Delete an object (will create delete marker) ######
    curl -XDELETE -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container/my_object" 
###### Really Delete and object ######
    curl -XDELETE -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container/my_object?version=all"     
###### Really Delete and object version ######
    curl -XDELETE -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container/my_object?version=1"   
###### Really Delete and multiple object version ######
    curl -XDELETE -u admin:admin "http://localhost:8092/openstackswift001/my_account/my_container/my_object?version=1,2,3" 
    
    
## Container Import and Export    

###### Export a container ######
    curl -XPOST -u admin:admin -H "x-sfs-dest-directory: /data/my_container_export" "http://localhost:8092/admin_containerexport/my_account/my_container"
###### Export a container and compress the export ######
    curl -XPOST -u admin:admin -H "x-sfs-dest-directory: /data/my_container_export" -H "x-sfs-compress: true" "http://localhost:8092/admin_containerexport/my_account/my_container"   
###### Export a container, compress and encrypt the export ######
    curl -XPOST -u admin:admin -H "x-sfs-dest-directory: /data/my_container_export" -H "x-sfs-compress: true" -H "x-sfs-secret: YWJjMTIzCg==" "http://localhost:8092/admin_containerexport/my_account/my_container"       
###### Import a container into a container named target_container ######
    curl -XPOST -u admin:admin -H "x-sfs-dest-directory: /data/my_container_export" "http://localhost:8092/admin_containerexport/my_account/target_containers"    
###### Import an encrypted container into a container named target_container ######
    curl -XPOST -u admin:admin -H "x-sfs-src-directory: /data/my_container_export" -H "x-sfs-secret: YWJjMTIzCg==" "http://localhost:8092/admin_containerexport/my_account/target_container"       
    
    
## Health Check

###### Your load balancers in front of sfs should use this url to see if sfs is alive ######
    curl -XGET -u admin:admin "http://localhost:8092/admin/001/healthcheck"
    
    
## Misc. Administration

###### Repair and object manually ######
    curl -XPOST -u admin:admin "http://localhost:8092/admin_objectrepair/my_account/my_account/my_container/my_object"
###### Verify and repair master keys (if amazon web services or azure spontaneously vanish from the face of planet earth) ######
    curl -XPOST -u admin:admin "http://localhost:8092/admin/001/master_keys_check"    
###### Manually run the maintenance jobs run by the master node ######
    curl -XPOST -u admin:admin "http://localhost:8092/admin/001/run_jobs"   
         
         
## Road Map
* Make the recycling block allocator more intelligent.
* Implement volume compaction so that disk space can be reclaimed if a massive delete is executed. For example: if you have a 1TB of data in volume and you delete 500GB of data it may be a good thing reclaim 500GB of disk space instead of waiting for the deleted blocks to be overwrriten with new data.
* Allow object data to be replicated to the same node as the primary and then have it spread across the cluster when more nodes come online. 
* Make the master key rotation inverval configurable
* Make the master key re-encryption interval configurable
* Make the container key rotation interval configurable
* Make the container key re-encryption interval configurable
* Improve container export functionality so that it can copy non local storage like sftp sites or cloud provider object storage
* Improve container export functionality to support differential exports
* Improve volume block allocation to pre-allocate space (fallocate)
* Make max upload size a configuration option