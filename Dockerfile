FROM python:3.14.8-alpine3.24@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72
LABEL org.opencontainers.image.source="https://github.com/m4rkireland/WOL-proxy"
LABEL org.opencontainers.image.description="Maintained MQTT-to-WoL relay with TLS, command safeguards and read-only health checks"
LABEL org.opencontainers.image.licenses="MIT"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt \
    && python -m pip uninstall --yes pip \
    && python -c "import ensurepip, shutil; shutil.rmtree(ensurepip.__path__[0])" \
    && addgroup -g 10001 relay \
    && adduser -D -u 10001 -G relay relay
COPY WOL-proxy.py wol_proxy.py mqtt_runner.py ./
USER 10001:10001
HEALTHCHECK --interval=20s --timeout=5s --start-period=40s --retries=3 CMD ["python", "mqtt_runner.py", "--healthcheck"]
ENTRYPOINT ["python", "WOL-proxy.py"]
