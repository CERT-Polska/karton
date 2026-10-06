FROM python:3.12-slim

WORKDIR /app/service
COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
RUN pip install pytest
COPY ./karton ./karton
COPY ./pyproject.toml ./pyproject.toml
RUN pip install .
ENTRYPOINT ["pytest"]
