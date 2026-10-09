FROM python:3.12-slim

WORKDIR /app/service
COPY ./requirements.txt ./requirements.gateway.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt -r requirements.gateway.txt

COPY pyproject.toml README.md ./
COPY karton ./karton

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install .[gateway]

ENTRYPOINT ["gunicorn"]
CMD ["-k", "karton.gateway.worker.GatewayASGIWorker", "-w", "1", "-b", "0.0.0.0:8000", "karton.gateway:app"]