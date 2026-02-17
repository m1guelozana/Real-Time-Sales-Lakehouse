FROM python:3.11-slim

WORKDIR /usr/app

RUN apt-get update && apt-get install -y git \
    && pip install dbt-postgres

CMD ["bash"]
