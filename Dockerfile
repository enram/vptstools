FROM python:3.9 as vpts

WORKDIR /app

COPY ./dist/*.whl /app
COPY requirements.txt /app/

RUN set -ex && \
    pip install -r requirements.txt &&\
    pip install s3fs &&\
    pip install $(ls -l | grep whl | awk {'print $9'})