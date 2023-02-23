FROM python:3.8

WORKDIR /app/service
COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt orjson
COPY ./karton ./karton
COPY ./README.md ./README.md
COPY ./setup.py ./setup.py
RUN pip install .
ENTRYPOINT ["karton-system"]
