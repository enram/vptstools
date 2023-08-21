FROM python:3.9 as vpts

WORKDIR /app

COPY dist/*.whl /app
COPY requirements.txt /app

RUN set -ex && \
    pip install -r requirements.txt &&\
    pip install /app/$(ls -l /app/ | grep whl | awk {'print $9'})
