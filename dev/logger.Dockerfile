FROM python:3.7

COPY requirements.txt /app/

RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY karton.ini.dev /etc/karton/karton.ini
COPY stdout_logger.py /app/

WORKDIR /app/

CMD python stdout_logger.py
