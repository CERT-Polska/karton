FROM python:3.12

WORKDIR /app/service
COPY ./karton ./karton
COPY ./README.md ./README.md
COPY ./pyproject.toml ./pyproject.toml
RUN pip install .[gateway]
ENTRYPOINT ["uvicorn", "--host", "0.0.0.0", "--port", "8000", "karton.gateway.app:app"]
