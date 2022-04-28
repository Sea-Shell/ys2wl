FROM python:3.10-alpine as base

RUN apk update && \
    apk add dumb-init && \
    adduser --disabled-password --uid 1000 --home /opt/ys2wl ys2wl && \
    pip install -qqq pipenv

FROM base as build

RUN apk add build-base libffi-dev openssl-dev libressl-dev musl-dev cargo # install pipenv dependencies
USER 1000
COPY Pipfile /opt/ys2wl/
WORKDIR /opt/ys2wl
RUN  pipenv install --three
COPY run /opt/ys2wl/
COPY *.py /opt/ys2wl/

FROM base as prod

ENV LC_ALL C.UTF-8
ENV PYTHONUNBUFFERED 1
WORKDIR /opt/ys2wl
USER 1000
COPY --from=build /opt/ys2wl /opt/ys2wl

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/opt/ys2wl/run"]
