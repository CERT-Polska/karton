FROM python:3.7

COPY requirements.txt /karton/
COPY setup.py /karton/
COPY karton/ /karton/karton/

RUN pip install --no-cache-dir /karton

COPY dev/stdout_logger.py /app/

WORKDIR /app/

CMD python stdout_logger.py
