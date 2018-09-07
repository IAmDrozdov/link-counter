FROM python:latest
ADD workflow_exercise /workflow_exercise/src
COPY requirements.txt /workflow_exercise
COPY db/init.sql /workflow/db
WORKDIR /workflow_exercise
RUN pip3 install -r requirements.txt
ENTRYPOINT ["python3", "src/__main__.py"]
