FROM tiangolo/meinheld-gunicorn:python3.8-alpine3.11

LABEL maintainer="Sebastian Ramirez <tiangolo@gmail.com>"

RUN pip install flask
RUN pip install neo4j

COPY ./app /app