FROM phusion/baseimage:latest

RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

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
