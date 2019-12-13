FROM phusion/baseimage:latest

# Install basic software support
RUN apt-get update && \
    apt-get install --yes software-properties-common && \
    mkdir -p /usr/lib/jvm

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ADD jdk-8u202-linux-x64.tar.gz /usr/lib/jvm

RUN mv /usr/lib/jvm/jdk1.8.0_202 /usr/lib/jvm/java-8-oracle

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle
ENV HEAP_SIZE 512m
ENV INSTANCES 1

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /data && mkdir -p /opt/sfs

COPY libs/ /opt/sfs
COPY vertx-conf.json /etc/vertx-conf.json
COPY vertx-logback.xml /etc/vertx-logback.xml

VOLUME /data/sfs

EXPOSE 80

CMD ["/sbin/my_init"]

RUN mkdir /etc/service/sfs
ADD sfs.sh /etc/service/sfs/run
ADD vertx.sh /etc/service/sfs/vertx.sh
