FROM python:2.7

LABEL maintainer="cert <info@cert.pl>"
COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

COPY . /karton/

WORKDIR /karton/
RUN python setup.py install
