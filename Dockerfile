FROM python:3.9 as vpts

WORKDIR /app

COPY . /app/

RUN set -ex && \
    pip install -r requirements.txt &&\
    pip install s3fs pipx &&\
    pipx run --spec tox==3.27.1 tox -e clean,build &&\
    pip install /app/$(ls -l /app/ | grep whl | awk {'print $9'})