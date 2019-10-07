FROM python:3.6-alpine

WORKDIR .

RUN apk add --no-cache --virtual .build-deps gcc libc-dev libxslt-dev && \
    apk add --no-cache libxslt && \
    pip install --no-cache-dir lxml>=3.5.0 && \
    apk del .build-deps

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt
