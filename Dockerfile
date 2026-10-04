FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONPYCACHEPREFIX=/tmp/pycache

WORKDIR /srv

COPY app ./app
COPY tests ./tests

RUN useradd --system --uid 10001 appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /data
USER appuser

EXPOSE 8080

# Default entrypoint; each compose service overrides the command.
CMD ["python", "-m", "app.control.server"]
