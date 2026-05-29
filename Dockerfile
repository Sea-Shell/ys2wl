FROM ghcr.io/astral-sh/uv:0.6 AS uv
FROM python:3.13-slim AS builder

COPY --from=uv /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen --no-install-project --no-group dev

COPY src/ src/
COPY ui/ ui/
RUN uv sync --frozen --no-group dev

FROM python:3.13-slim AS runtime

ARG UID=1000
ARG GID=1000
ARG APP_PORT=8080
ARG APP_VERSION=dev
ARG BUILD_DATE
ARG VCS_REF

LABEL org.opencontainers.image.source="https://github.com/Sea-Shell/ys2wl" \
  org.opencontainers.image.description="ys2wl" \
  org.opencontainers.image.licenses="MIT" \
  org.opencontainers.image.version="${APP_VERSION}" \
  org.opencontainers.image.created="${BUILD_DATE}" \
  org.opencontainers.image.revision="${VCS_REF}"

RUN apt-get update && apt-get install -y --no-install-recommends tini \
  && apt-get clean && rm -rf /var/lib/apt/lists/* \
  && addgroup --gid ${GID} appgroup \
  && adduser --disabled-password --uid ${UID} --gid ${GID} --gecos "" appuser

WORKDIR /app

COPY --from=builder --chown=${UID}:${GID} /app /app

ENV PATH="/app/.venv/bin:$PATH" \
  YS2WL_NO_WEBBROWSER=true \
  PYTHONUNBUFFERED=1

EXPOSE ${APP_PORT}

USER appuser

ENTRYPOINT ["tini", "--"]
CMD ["python", "-m", "ys2wl"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; exit(0) if urllib.request.urlopen('http://localhost:8080/api/health').status == 200 else exit(1)"
