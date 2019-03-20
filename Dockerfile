FROM python:3.6

LABEL maintainer="des <des@cert.pl>"
COPY requirements.txt /tmp/requirements.txt

RUN pip3 install -r /tmp/requirements.txt

COPY . /karton/

WORKDIR /karton/
RUN python3 setup.py install
