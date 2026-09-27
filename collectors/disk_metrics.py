import os
import psutil
import logging


# ---------- Discovery ----------


def publish_discovery(agent):
    mounts = agent.config.get("mounts", {})
    if not mounts:
        logging.warning("No mounts defined in config.yaml")
        return

    for name in mounts:
        safe = _safe(name)

        _publish_sensor(
            agent,
            key=f"disk_{safe}_used_percent",
            name=f"{name} Used",
            unit="%",
            device_class=None,
            state_class="measurement",
            icon="harddisk",
        )

        _publish_sensor(
            agent,
            key=f"disk_{safe}_free_gb",
            name=f"{name} Free",
            unit="GB",
            device_class=None,
            state_class="measurement",
            icon="harddisk",
        )


def _publish_sensor(agent, key, name, unit, device_class, state_class, icon):
    topic = (
        f"{agent.discovery_prefix}/sensor/"
        f"{agent.device}/{key}/config"
    )

    payload = {
        "name": name,
        "state_topic": agent.state_topic,
        "value_template": f"{{{{ value_json.{key} }}}}",
        "unique_id": f"{agent.device}_{key}",
        "device": agent.device_info,
        "unit_of_measurement": unit,
        "state_class": state_class,
        "icon": f"mdi:{icon}",
    }

    if device_class:
        payload["device_class"] = device_class

    agent.mqtt.publish(topic, agent.json(payload), retain=True)


# ---------- State collection ----------


def collect(agent):
    data = {}
    mounts = agent.config.get("mounts", {})

    for name, path in mounts.items():
        if not os.path.exists(path):
            logging.warning(f"Mount path missing: {name} ({path})")
            continue

        try:
            usage = psutil.disk_usage(path)
            safe = _safe(name)
            data[f"disk_{safe}_used_percent"] = round(usage.percent, 1)
            data[f"disk_{safe}_free_gb"] = round(usage.free / (1024 ** 3), 2)
        except Exception as e:
            logging.error(f"Disk metric failed for {name}: {e}")

    return data


# ---------- Helpers ----------


def _safe(name: str) -> str:
    return name.lower().replace(" ", "_")
