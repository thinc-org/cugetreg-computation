FROM docker.io/python:3.9-slim

WORKDIR /usr/src/app

# gRPC probe for liveness exec check with: grpc_health_probe -addr=:50051
RUN apt-get update
RUN apt-get install -y curl
RUN curl -o /bin/grpc_health_probe -L 'https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.19/grpc_health_probe-linux-amd64'
RUN chmod +x /bin/grpc_health_probe

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY config.production.ini ./config.ini

EXPOSE 50051

CMD [ "python", "-m", "cgrcompute.server"]
