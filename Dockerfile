FROM python:3.9 as vpts

WORKDIR /app

COPY ./dist/*.whl /app
COPY requirements.txt /app/

RUN set -ex && \
    pip install -r requirements.txt &&\
    pip install vptstools-0.0.post1.dev57+g1d2fc15.d20230403-py3-none-any.whl  # TAKE OUTPUT CI BUILD

CMD ["vph5_to_vpts", "--modified-days-ago", "2"]
