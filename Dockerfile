FROM python:3.8
# Copies your code files from your action repository to the filesystem path `/opt/app` of the container
COPY requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app
RUN pip install -r requirements.txt
COPY . /opt/app
# Code file to execute when the docker container starts up (`entrypoint.sh`)
ENTRYPOINT ["/opt/app/entrypoint.sh"]