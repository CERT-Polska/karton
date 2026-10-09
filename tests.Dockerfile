FROM python:3.12-slim

WORKDIR /app/service
COPY ./requirements.txt ./requirements.txt
COPY ./tests/requirements.txt ./tests-requirements.txt
RUN pip install -r requirements.txt
RUN pip install -r tests-requirements.txt
COPY ./karton ./karton
COPY ./pyproject.toml ./pyproject.toml
RUN pip install .
ENTRYPOINT ["pytest"]
