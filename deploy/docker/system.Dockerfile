FROM dr.cert.pl/karton/karton:python3

WORKDIR /usr/src/app

COPY "main.py" ./
COPY "config.py" ./
COPY "requirements.txt" ./

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
