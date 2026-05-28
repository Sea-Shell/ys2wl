FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS builder

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml .
RUN uv sync --no-install-project --no-dev

COPY src/ src/

FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS runtime

RUN pip install --no-cache-dir uv \
    && adduser --disabled-password --uid 1000 --gecos "" appuser

WORKDIR /app

COPY --from=builder /app /app

ENV YS2WL_NO_WEBBROWSER=true
ENV PYTHONUNBUFFERED=1
ENV PATH="/root/.local/bin:${PATH}"

EXPOSE 8080

USER 1000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/api/health')" || exit 1

CMD ["uv", "run", "python", "-m", "ys2wl"]
