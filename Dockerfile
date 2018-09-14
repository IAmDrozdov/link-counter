FROM python:3.6

COPY requirements.txt .
RUN pip3 install -r requirements.txt

ADD workflow_exercise /workflow_exercise/src
WORKDIR /workflow_exercise
COPY workflow_exercise/luigi.cfg /etc/luigi/
COPY jars/mysql-connector-java-8.0.12.jar /usr/local/spark/jars/mysql-connector-java-8.0.12.jar
COPY db ./db

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENTRYPOINT ["python3", "src/all.py"]
