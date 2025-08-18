FROM python:3.12

WORKDIR /app/service
COPY ./karton ./karton
COPY ./README.md ./README.md
COPY ./pyproject.toml ./pyproject.toml
RUN pip install .
ENTRYPOINT ["karton-system"]
