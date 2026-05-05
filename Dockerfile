FROM python:3.15.0a8-alpine@sha256:67a4b22b18852de8a9f668937641d2926e2dc9832b4a24ed29c9f5afb125251d AS base

RUN apk update && \
  apk add dumb-init && \
  adduser --disabled-password --uid 1000 --home /opt/ys2wl ys2wl

FROM base AS build
COPY --from=ghcr.io/astral-sh/uv:latest@sha256:6b6fa841d71a48fbc9e2c55651c5ad570e01104d7a7d701f57b2b22c0f58e9b1 /uv /uvx /bin/
RUN apk update && \
    apk add --no-cache build-base cmake ninja
ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python
USER 1000
COPY pyproject.toml /opt/ys2wl/
COPY uv.lock /opt/ys2wl/
WORKDIR /opt/ys2wl
RUN uv venv
RUN uv sync --locked --no-install-project --no-dev
COPY run /opt/ys2wl/
COPY *.py /opt/ys2wl/

FROM base AS prod
ENV LC_ALL=C.UTF-8
ENV PYTHONUNBUFFERED=1
WORKDIR /opt/ys2wl
USER 1000
COPY --from=build /opt/ys2wl /opt/ys2wl
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/opt/ys2wl/run"]
