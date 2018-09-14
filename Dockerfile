FROM python:3.6
ADD workflow_exercise /workflow_exercise/src
WORKDIR /workflow_exercise
COPY requirements.txt .
COPY db ./db
RUN pip3 install -r requirements.txt

RUN \
  apt update && apt-get install -y \
  openjdk-8-jdk \
  ssh \
  rsync \
  openssh-server
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH $PATH:$JAVA_HOME/bin

RUN \
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
  chmod 0600 ~/.ssh/authorized_keys
# # # # # # # # # #

# Setting up Hadoop
ENV HADOOP_HOME /usr/local/hadoop

RUN \
  curl http://www-eu.apache.org/dist/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz | \
  tar -xz -C /usr/local && \
  ln -s /usr/local/hadoop-3.1.1 $HADOOP_HOME


ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin


RUN \
  echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
  echo "PATH=$PATH:$HADOOP_HOME/bin" >> ~/.bashrc


COPY hadoop/* $HADOOP_HOME/etc/hadoop/

RUN /etc/init.d/ssh start

ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"
# # # # # # # # # #

# Setting up Spark
ENV SPARK_HOME=/usr/local/spark

RUN \
  curl http://www-eu.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz | \
  tar -xz -C /usr/local && \
  ln -s /usr/local/spark-2.3.1-bin-hadoop2.7 $SPARK_HOME

COPY jars/* $SPARK_HOME/jars


ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
EXPOSE 9870 9000 8088 8025 8030 8050
WORKDIR /
ENTRYPOINT ["python3", "workflow_exercise/src/testing.py"]
