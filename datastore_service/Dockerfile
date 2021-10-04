# Dockerfile

# Pull base image
FROM python:3.8-slim

# Set environment varibles
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY . /datastore
WORKDIR /datastore

# RUN mkdir _docker

RUN apt-get update && apt-get upgrade -y

RUN pip3 install -r requirements.txt

CMD ["./startup.sh"]

# Start Luigi
CMD ["luigid --port 8112"]
CMD ["python","etl_worker.py"]
