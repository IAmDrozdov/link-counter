FROM python:latest
ADD workflow_exercise /workflow_exercise/src
COPY requirements.txt /workflow_exercise
COPY db/init.sql /db
RUN pip3 install -r workflow_exercise/requirements.txt
ENTRYPOINT ["python3", "workflow_exercise/src/__main__.py"]
