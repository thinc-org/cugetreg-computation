FROM docker.io/python:3.9-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY config.production.ini ./config.ini

EXPOSE 50051

CMD [ "python", "-m", "cgrcompute.server"]
