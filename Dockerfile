FROM python:latest
ADD workflow_exercise/ /workflow_exercise
COPY requirements.txt /
RUN pip3 install -r requirements.txt
CMD ["python", "/workflow_exercise/__main__.py"]
