import json
import re
import subprocess
import logging
from datetime import datetime, timezone


# ---------- Discovery ----------


def publish_discovery(agent):
    services = agent.config.get("services", [])
    if not services:
        logging.warning("No services defined in config.yaml")
        return

    # Individual sensor per service
    for service in services:
        safe = _safe(service)

        _publish_sensor(
            agent,
            key=f"service_{safe}",
            name=f"{service}",
            icon="cog",
        )

    # Overall binary sensor — ON if any service is down
    _publish_binary_sensor(
        agent,
        key="services_status",
        name="Services Status",
        device_class="problem",
        icon="alert-circle",
    )

    # Agent metrics — data is in sensor state payload, grouped under Services
    _publish_agent_metric(
        agent,
        key="agent_uptime",
        name="Agent Uptime",
        unit="s",
        state_class="measurement",
        icon="clock-outline",
    )

    _publish_agent_metric(
        agent,
        key="agent_cycle_ms",
        name="Agent Cycle Time",
        unit="ms",
        state_class="measurement",
        icon="timer-cog",
    )


def _publish_sensor(agent, key, name, icon):
    topic = (
        f"{agent.discovery_prefix}/sensor/"
        f"{agent.device}/{key}/config"
    )

    payload = {
        "name": name,
        "state_topic": agent.service_state_topic,
        "value_template": f"{{{{ value_json['{key}'] }}}}",
        "unique_id": f"{agent.device}_{key}",
        "device": agent.service_device_info,
        "icon": f"mdi:{icon}",
    }

    agent.mqtt.publish(topic, json.dumps(payload), retain=True)


def _publish_binary_sensor(agent, key, name, device_class, icon):
    topic = (
        f"{agent.discovery_prefix}/binary_sensor/"
        f"{agent.device}/{key}/config"
    )

    payload = {
        "name": name,
        "state_topic": agent.service_state_topic,
        "value_template": f"{{{{ value_json['{key}'] }}}}",
        "unique_id": f"{agent.device}_{key}",
        "device": agent.service_device_info,
        "device_class": device_class,
        "icon": f"mdi:{icon}",
    }

    agent.mqtt.publish(topic, json.dumps(payload), retain=True)


def _publish_agent_metric(agent, key, name, unit, state_class, icon):
    """Publish discovery for agent metrics — reads from sensor state topic, grouped under Services."""
    topic = (
        f"{agent.discovery_prefix}/sensor/"
        f"{agent.device}/{key}/config"
    )

    payload = {
        "name": name,
        "state_topic": agent.state_topic,
        "value_template": f"{{{{ value_json['{key}'] }}}}",
        "unique_id": f"{agent.device}_{key}",
        "device": agent.service_device_info,
        "unit_of_measurement": unit,
        "state_class": state_class,
        "icon": f"mdi:{icon}",
    }

    agent.mqtt.publish(topic, json.dumps(payload), retain=True)


# ---------- State collection ----------


def collect(agent):
    data = {}
    services = agent.config.get("services", [])
    any_down = False

    for service in services:
        safe = _safe(service)
        status = _check_service(service)
        data[f"service_{safe}"] = status

        if status != "active":
            any_down = True

    # Overall status: ON = problem (something down), OFF = all good
    data["services_status"] = "ON" if any_down else "OFF"

    return data


# ---------- Helpers ----------


def _check_service(service_name: str) -> str:
    """Check service status via systemctl is-active."""
    try:
        result = subprocess.run(
            ["systemctl", "is-active", f"{service_name}.service"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip()
    except Exception as e:
        logging.error(f"Failed to check service {service_name}: {e}")
        return "unknown"


def _safe(name: str) -> str:
    """Sanitize a name for use as an MQTT discovery object_id and a Jinja
    value_template attribute.

    HA's discovery topic requires node_id/object_id to match [a-zA-Z0-9_-],
    and value_template's dot-notation (value_json.KEY) requires KEY to be a
    valid identifier — neither tolerates the real service name as-is (e.g.
    "vncserver@:1"). Rather than special-case each offending character as it
    comes up, collapse anything outside [a-zA-Z0-9_] to a single underscore,
    so new oddities (systemd instance names, unit suffixes, whatever comes
    next) are handled the same way without another patch here.
    """
    safe = re.sub(r"[^a-zA-Z0-9_]+", "_", name.strip().lower())
    safe = safe.strip("_")
    if not safe:
        safe = "unnamed"
    if safe[0].isdigit():
        safe = f"_{safe}"
    return safe