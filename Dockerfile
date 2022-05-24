FROM python:3.7

WORKDIR /app/service
COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
COPY ./karton ./karton
COPY ./README.md ./README.md
COPY ./setup.py ./setup.py
RUN pip install .
ENTRYPOINT ["karton-system"]
