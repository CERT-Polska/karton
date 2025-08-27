FROM python:3.11

WORKDIR /app/service
COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
COPY ./karton ./karton
COPY ./README.md ./README.md
COPY ./pyproject.toml ./pyproject.toml
RUN pip install .
ENTRYPOINT ["karton-system"]
