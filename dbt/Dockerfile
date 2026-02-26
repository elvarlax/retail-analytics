FROM python:3.11-slim

WORKDIR /usr/app

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["dbt"]
