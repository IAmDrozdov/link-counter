FROM python:latest
ADD workflow_exercise /workflow_exercise/src
COPY requirements.txt /workflow_exercise
COPY db /workflow/db
WORKDIR /workflow_exercise

# Setting up Java and SSH
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

#RUN sed -i '/^export JAVA_HOME/ s:.*:export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/\nexport HADOOP_PREFIX=/usr/local/hadoop\nexport HADOOP_HOME=/usr/local/hadoop\n:' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
#RUN sed -i '/^export HADOOP_CONF_DIR/ s:.*:export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop/:' $HADOOP_HOME/etc/hadoop/hadoop-env.sh

COPY hadoop/* $HADOOP_HOME/etc/hadoop/
#
#ENV HDFS_NAMENODE_USER="root"
#ENV HDFS_DATANODE_USER="root"
#ENV HDFS_SECONDARYNAMENODE_USER="root"
#ENV YARN_RESOURCEMANAGER_USER="root"
#ENV YARN_NODEMANAGER_USER="root"


RUN /etc/init.d/ssh start

ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

RUN $HADOOP_HOME/bin/hdfs namenode -format

RUN $HADOOP_HOME/sbin/start-dfs.sh
RUN $HADOOP_HOME/sbin/start-yarn.sh

RUN $HADOOP_HOME/bin/hdfs dfs -mkdir tmp
# # # # # # # # # #

# Setting up Spark
ENV SPARK_HOME=/usr/local/spark

RUN \
  curl http://www-eu.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz | \
  tar -xz -C /usr/local

RUN ln -s /usr/local/spark-2.3.1-bin-hadoop2.7 $SPARK_HOME


ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

RUN pip3 install -r requirements.txt

EXPOSE 9870 9000 8088 8025 8030 8050

ENTRYPOINT ["python3", "src/__main__.py"]
