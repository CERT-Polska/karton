FROM python:3.12-slim

WORKDIR /app/service
COPY ./requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

COPY pyproject.toml README.md ./
COPY karton ./karton

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install .

ENTRYPOINT ["karton-system"]
