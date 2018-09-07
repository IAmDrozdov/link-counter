FROM python:latest
ADD workflow_exercise /workflow_exercise/src
COPY requirements.txt /workflow_exercise
RUN pip3 install -r workflow_exercise/requirements.txt
CMD ["python3", "workflow_exercise/src/__main__.py"]
