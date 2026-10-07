FROM python:3.14.8-slim-trixie@sha256:f85c5697265c178cc6887276c55fe16cf3d14ca35c3df6a5eab3b360534a55d2
LABEL org.opencontainers.image.source="https://github.com/m4rkireland/WOL-proxy"
LABEL org.opencontainers.image.description="Maintained MQTT-to-WoL relay with TLS, command safeguards and read-only health checks"
LABEL org.opencontainers.image.licenses="MIT"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt && groupadd --gid 10001 relay && useradd --uid 10001 --gid relay --no-create-home relay
COPY WOL-proxy.py wol_proxy.py mqtt_runner.py ./
USER 10001:10001
HEALTHCHECK --interval=20s --timeout=5s --start-period=40s --retries=3 CMD ["python", "mqtt_runner.py", "--healthcheck"]
ENTRYPOINT ["python", "WOL-proxy.py"]
