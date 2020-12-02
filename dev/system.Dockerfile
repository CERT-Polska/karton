FROM python:3.7

COPY requirements.txt /karton/
COPY setup.py /karton/
COPY karton/ /karton/karton/

RUN pip install --no-cache-dir /karton

COPY dev/karton.ini.dev /etc/karton/karton.ini

CMD karton-system --setup-bucket
