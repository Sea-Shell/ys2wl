FROM python:3.10-alpine as base

RUN apk update && \
    apk add dumb-init && \
    adduser --disabled-password --uid 1000 --home /opt/ysl ysl && \
    pip install -qqq pipenv

FROM base as build

RUN apk add build-base libffi-dev openssl-dev libressl-dev musl-dev cargo # install pipenv dependencies
USER 1000
COPY Pipfile /opt/ysl/
WORKDIR /opt/ysl
RUN  pipenv install
COPY run /opt/ysl/
COPY *.py /opt/ysl/

FROM base as prod

ENV LC_ALL C.UTF-8
ENV PYTHONUNBUFFERED 1
ENV LOG_LEVEL INFO
WORKDIR /opt/ysl
USER 1000
COPY --from=build /opt/ysl /opt/ysl

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/opt/ysl/run"]