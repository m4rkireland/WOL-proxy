# Maintained WOL-proxy

Fork of [seanauff/WOL-proxy](https://github.com/seanauff/WOL-proxy), MIT. MQTT command -> local-network Wake-on-LAN; no HTTP listener or privileged/raw socket is needed.

## Image and network

`ghcr.io/m4rkireland/wol-proxy:latest` (amd64/arm64). Production should select an immutable revision/digest. Builds pass unit tests, a real local Mosquitto integration test with a mocked packet sender, and a fixed HIGH/CRITICAL vulnerability gate before publication.

**The container must be connected to the destination VLAN** (macvlan/ipvlan or an appropriate host interface). Ordinary Docker bridge or host networking on a management-only host does not solve cross-VLAN broadcast delivery. This service never reconfigures host networking.

Run as UID/GID 10001, drop all capabilities, use a read-only root filesystem and a writable `/tmp` tmpfs. Credentials may be supplied through a Docker secret file. No ports are exposed. Health checks inspect broker subscription/network readiness and never send a wake packet.

## MQTT contract

- `<MQTT_TOPIC_PREFIX>/command`: an ASCII MAC address, published **without retain**.
- `<prefix>/status`: retained `Online` when broker subscription and optional source-network guard are ready; last will/graceful shutdown uses `Offline`. Readiness updates use QoS 0 (no broker acknowledgement) to avoid Paho's QoS 1 reconnect replay queue. Failed publishes are retried on the next tick, and readiness is recomputed after every reconnect. The last will and graceful-shutdown `Offline` remain QoS 1; command QoS is unchanged.
- `<prefix>/result`: non-retained JSON such as `{"status":"sent"}`. This proves send dispatch, **not** that a sleeping computer woke.

MQTT v5 is used. Retained commands are suppressed both at initial subscription and during a live subscription. Invalid/oversized payloads and wrong topics are rejected. An optional MAC allowlist limits targets. A three-second cooldown serializes duplicate/concurrent requests. There are no commands on startup, reconnect or health checks.

## Configuration

Existing variable names remain supported:

- `MQTT_BROKER_HOST=127.0.0.1`, `MQTT_BROKER_PORT=1883` (8883 when TLS enabled)
- `MQTT_CLIENT_ID=WOL-proxy`, `MQTT_TOPIC_PREFIX=WOL-proxy`, `MQTT_QOS=1`
- `MQTT_USERNAME`, `MQTT_PASSWORD` or preferred `MQTT_PASSWORD_FILE`
- `WOL_BROADCAST_ADDR=255.255.255.255`, `WOL_PORT=9`

Additional safeguards:

- `MQTT_TLS=true`: verify the server certificate and hostname using system CA trust; `MQTT_TLS_CA_FILE` supports a private CA. There is no insecure TLS option.
- `WOL_ALLOWED_MACS`: comma-separated target MAC allowlist. Empty preserves generic upstream behavior; production should set it.
- `WOL_SOURCE_INTERFACE`: interface from which to read the IPv4 source, or `WOL_SOURCE_IP` for an explicit address.
- `WOL_SOURCE_SUBNET`: require the selected source address and configured broadcast to belong to the expected IPv4 network. Requires a source interface/IP.

To connect to a private broker address while validating its public certificate name, use a container-scoped hostname mapping; do not disable verification.

## Home Assistant

Keep the existing script/dashboard control, replacing only its action:

```yaml
sequence:
  - action: mqtt.publish
    data:
      topic: wol/example/command
      payload: "02:00:00:00:00:01"
      qos: 1
      retain: false
```

Configure the relay prefix, allowlist and destination VLAN accordingly. For non-Work deployments, substitute your actual target and topic.

## Tests and maintenance

`python -m pip install -r requirements.txt && python -m unittest -v`

The broker integration test requires a local `mosquitto` executable. It listens only on localhost and replaces the actual packet sender with a mock. CI installs Mosquitto and executes this test. `python mqtt_runner.py --healthcheck` performs read-only health inspection.

The Python base/dependencies/Actions are pinned. Renovate is configured for daily checks with a two-day release age and green-only dependency automerge; installation/runner access is separate from this repository configuration. Unfixed CVEs are reported by Trivy but do not bypass fixed HIGH/CRITICAL gates.
