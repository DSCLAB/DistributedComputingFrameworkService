FROM cdclab/hadoop-nodemanager

RUN apt-get update && DEBIAN_FRONTEND=noninteractive && apt-get install -y --no-install-recommends \
      build-essential \
      libkrb5-dev \
      wget \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && DEBIAN_FRONTEND=noninteractive && apt-get install -y --no-install-recommends \
      python2-minimal \
      python3.9 \
      python3-dev \
      python3-pip \
      python-is-python3 \
      alien \
      libaio1 \
      freetds-dev \
      freetds-bin \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1
RUN update-alternatives --set python3 /usr/bin/python3.10
RUN pip3 install --upgrade pip

RUN wget -O /tmp/oracle-instantclient-basic-21.4.0.0.0-1.x86_64.rpm https://download.oracle.com/otn_software/linux/instantclient/214000/oracle-instantclient-basic-21.4.0.0.0-1.x86_64.rpm
RUN alien -i /tmp/oracle-instantclient-basic-21.4.0.0.0-1.x86_64.rpm

ADD requirements.txt /requirements.txt
RUN pip3 install -r requirements.txt

RUN wget -O /tmp/phoenix-hbase-2.4-5.1.2-bin.tar.gz https://archive.apache.org/dist/phoenix/phoenix-5.1.2/phoenix-hbase-2.4-5.1.2-bin.tar.gz
RUN tar -xvf /tmp/phoenix-hbase-2.4-5.1.2-bin.tar.gz -C /opt/ \
    && rm /tmp/phoenix-hbase-2.4-5.1.2-bin.tar.gz
ENV PHOENIX_HOME=/opt/phoenix-hbase-2.4-5.1.2-bin
ENV PATH $PHOENIX_HOME/bin/:$PATH

# install spark
ENV BASE_URL=https://archive.apache.org/dist/spark
ENV SPARK_VERSION=3.3.1
ENV HADOOP_VERSION=3

RUN chmod +x *.sh \
      && wget ${BASE_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && cd /

# spark setting file
COPY /spark-env.sh /spark/conf/

ADD run.sh /run.sh
RUN chmod a+x /run.sh

RUN mkdir -p /tmp/dcfs

CMD ["/run.sh"]
