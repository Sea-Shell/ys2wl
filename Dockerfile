FROM python:3.15.0b1-alpine@sha256:8fe37a98f64ee15fee985b1536cc005aef67847c31f435772250165307a68c2e AS base

RUN apk update && \
  apk add dumb-init && \
  adduser --disabled-password --uid 1000 --home /opt/ys2wl ys2wl

FROM base AS build
COPY --from=ghcr.io/astral-sh/uv:latest@sha256:440fd6477af86a2f1b38080c539f1672cd22acb1b1a47e321dba5158ab08864d /uv /uvx /bin/
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
