FROM python:latest
ADD workflow_exercise /workflow_exercise/src
WORKDIR /workflow_exercise
COPY requirements.txt .
COPY jars ./jars
COPY db ./db
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "src/testing.py"]
