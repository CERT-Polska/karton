FROM python:3.12-slim

WORKDIR /app/service
COPY ./requirements.txt ./requirements.gateway.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt -r requirements.gateway.txt

COPY pyproject.toml README.md ./
COPY karton ./karton

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install .[gateway]

ENTRYPOINT ["uvicorn"]
CMD ["karton.gateway:app", "--host", "0.0.0.0", "--port", "8000"]