FROM phusion/baseimage:latest

RUN apt-get update && apt-get upgrade -y -o Dpkg::Options::="--force-confold"

RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle
ENV HEAP_SIZE 512m
ENV INSTANCES 1

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN groupadd -r sfs && useradd -r -g sfs sfs
RUN mkdir -p /data && mkdir -p /opt/sfs

COPY libs/ /opt/sfs
COPY vertx-logback.xml /etc/vertx-logback.xml

RUN chown -R sfs:sfs /data && chown -R sfs:sfs /opt/sfs && chown sfs:sfs /etc/vertx-logback.xml

CMD ["/sbin/my_init"]

RUN mkdir /etc/service/sfs
ADD sfs.sh /etc/service/sfs/run
ADD vertx.sh /etc/service/sfs/vertx.sh
