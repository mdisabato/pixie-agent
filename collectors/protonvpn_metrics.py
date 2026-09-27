#!/usr/bin/env python3
"""ProtonVPN metrics collector — grouped as a standalone "ProtonVPN" device
(via_device ties it to the host device in HA).

Entities:
  - protonvpn_connected   : nmcli STATE field for the ProtonVPN wireguard
                            connection (activated/not) — NM's own self-report,
                            not independently verified against actual traffic
  - protonvpn_server      : nmcli connection NAME, same query, same loop
  - protonvpn_app_running : pgrep check on the GUI process
  - protonvpn_rx_rate     : download rate (B/s), sysfs byte-counter delta
  - protonvpn_tx_rate     : upload rate (B/s), sysfs byte-counter delta

Connected, server, and the tunnel interface name all come from a single
nmcli call — no second query, no dependency on a wg-level check (none
currently exists in agent.py).

Rate calculation needs two samples: each collect() reads the current
sysfs byte counters, diffs against the previous cycle's reading (held in
module-level _last_sample), and divides by elapsed time. First cycle after
agent start has no prior sample, so rates report 0 until the second poll.
A negative delta (interface torn down/recreated on VPN reconnect, counters
reset to 0) is treated the same way — 0 for that one cycle rather than a
garbage negative number.

Deliberately NOT included (available as later opt-in additions if wanted):
  - kill switch state
  - split_tunneling daemon systemd status
  - swapping protonvpn_connected to be rate-derived instead of NM-derived
    (flagged as a real option, not done here — a genuinely idle-but-healthy
    tunnel looks identical to a stalled one under a pure rate check)

Known limitation: since protonvpn_connected is NM's self-reported state
rather than an independent wg-level check, it can say "activated" even if
the tunnel has silently stalled. If that gap matters to you, a real fix is
a separate addition — `sudo wg` output, parsed for handshake recency —
which would need a passwordless sudoers entry for the agent user. That's
new work, not something already in place; happy to build it if wanted.
"""

import json
import logging
import subprocess
import time

APP_PROCESS_MATCH = "/usr/bin/protonvpn-app"

# Holds the previous sample per interface for rate calculation:
# {iface: {"rx": int, "tx": int, "time": float}}
_last_sample = {}


# ---------- Device grouping ----------

def _device_info(agent):
    return {
        "identifiers":  [f"{agent.device}_protonvpn"],
        "name":         f"{agent.device} Proton VPN",
        "manufacturer": "Proton AG",
        "sw_version":   _installed_version(),
        # Ties to the real "{device} Sensors" device (identifiers:
        # [f"{device}_sensor"] in agent.py) — there's no standalone
        # bare-hostname device to reference.
        "via_device":   f"{agent.device}_sensor",
    }


def _installed_version() -> str:
    """Installed proton-vpn-gtk-app version via dpkg."""
    try:
        result = subprocess.run(
            ["dpkg-query", "-W", "-f=${Version}", "proton-vpn-gtk-app"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        return "unknown"
    except Exception as e:
        logging.error(f"Failed to check proton-vpn-gtk-app version: {e}")
        return "unknown"


# ---------- Discovery ----------

def publish_discovery(agent):
    device = _device_info(agent)

    _publish_binary_sensor(
        agent, device,
        key="protonvpn_connected",
        name=f"{agent.device} Connected",
        device_class="connectivity",
        icon="vpn",
    )
    _publish_binary_sensor(
        agent, device,
        key="protonvpn_app_running",
        name=f"{agent.device} App Running",
        device_class="running",
        icon="application",
    )
    _publish_sensor(
        agent, device,
        key="protonvpn_server",
        name=f"{agent.device} Server",
        icon="server-network",
    )
    _publish_sensor(
        agent, device,
        key="protonvpn_rx_rate",
        name=f"{agent.device} Download Rate",
        icon="download-network",
        unit="B/s",
        device_class="data_rate",
        state_class="measurement",
    )
    _publish_sensor(
        agent, device,
        key="protonvpn_tx_rate",
        name=f"{agent.device} Upload Rate",
        icon="upload-network",
        unit="B/s",
        device_class="data_rate",
        state_class="measurement",
    )


def _publish_sensor(agent, device, key, name, icon, unit=None, device_class=None, state_class=None):
    topic = f"{agent.discovery_prefix}/sensor/{agent.device}/{key}/config"
    payload = {
        "name":           name,
        "state_topic":    agent.state_topic,
        "value_template": f"{{{{ value_json.{key} }}}}",
        "unique_id":      f"{agent.device}_{key}",
        "default_entity_id": f"sensor.{agent.device}_{key}",
        "device":         device,
        "icon":           f"mdi:{icon}",
    }
    if unit is not None:
        payload["unit_of_measurement"] = unit
    if device_class is not None:
        payload["device_class"] = device_class
    if state_class is not None:
        payload["state_class"] = state_class
    agent.mqtt.publish(topic, json.dumps(payload), retain=True)


def _publish_binary_sensor(agent, device, key, name, device_class, icon):
    topic = f"{agent.discovery_prefix}/binary_sensor/{agent.device}/{key}/config"
    payload = {
        "name":           name,
        "state_topic":    agent.state_topic,
        "value_template": f"{{{{ value_json.{key} }}}}",
        "unique_id":      f"{agent.device}_{key}",
        "default_entity_id": f"binary_sensor.{agent.device}_{key}",
        "device":         device,
        "device_class":   device_class,
        "icon":           f"mdi:{icon}",
    }
    agent.mqtt.publish(topic, json.dumps(payload), retain=True)


# ---------- Collection ----------

def collect(agent):
    data = {
        "protonvpn_connected":   "OFF",
        "protonvpn_app_running": "OFF",
        "protonvpn_server":      "unknown",
        "protonvpn_rx_rate":     0,
        "protonvpn_tx_rate":     0,
    }

    data["protonvpn_app_running"] = "ON" if _app_process_running() else "OFF"

    # Single nmcli query supplies connected state, server name, and the
    # actual interface name (not hardcoded — read from nmcli's DEVICE field).
    for name, conn_type, device, state in _nmcli_active_connections():
        if conn_type == "wireguard" and name.startswith("ProtonVPN"):
            data["protonvpn_connected"] = "ON" if state == "activated" else "OFF"
            data["protonvpn_server"] = name.replace("ProtonVPN", "", 1).strip()

            rx_rate, tx_rate = _compute_rates(device)
            if rx_rate is not None:
                data["protonvpn_rx_rate"] = round(rx_rate, 1)
                data["protonvpn_tx_rate"] = round(tx_rate, 1)
            break

    return data


# ---------- Helpers ----------

def _app_process_running() -> bool:
    try:
        result = subprocess.run(
            ["pgrep", "-f", APP_PROCESS_MATCH],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except Exception as e:
        logging.error(f"Failed to check protonvpn-app process: {e}")
        return False


def _read_interface_bytes(iface):
    """Reads cumulative rx/tx byte counters from sysfs. Root-free."""
    try:
        with open(f"/sys/class/net/{iface}/statistics/rx_bytes") as f:
            rx = int(f.read().strip())
        with open(f"/sys/class/net/{iface}/statistics/tx_bytes") as f:
            tx = int(f.read().strip())
        return rx, tx
    except Exception as e:
        logging.error(f"Failed to read interface stats for {iface}: {e}")
        return None, None


def _compute_rates(iface):
    """Returns (rx_bytes_per_sec, tx_bytes_per_sec), or (None, None) if no
    prior sample exists yet, or if the interface's counters reset (torn
    down/recreated, e.g. VPN reconnect) since the last read.
    """
    now = time.time()
    rx, tx = _read_interface_bytes(iface)
    if rx is None:
        return None, None

    prev = _last_sample.get(iface)
    _last_sample[iface] = {"rx": rx, "tx": tx, "time": now}

    if prev is None:
        return None, None  # first sample since agent start — no delta yet

    elapsed = now - prev["time"]
    if elapsed <= 0:
        return None, None

    rx_delta = rx - prev["rx"]
    tx_delta = tx - prev["tx"]

    if rx_delta < 0 or tx_delta < 0:
        return None, None  # counters reset — treat as no rate this cycle

    return rx_delta / elapsed, tx_delta / elapsed


def _nmcli_active_connections():
    """Returns (name, type, device, state) tuples for active NM connections."""
    try:
        result = subprocess.run(
            ["nmcli", "-t", "-f", "NAME,TYPE,DEVICE,STATE", "connection", "show", "--active"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        connections = []
        for line in result.stdout.strip().splitlines():
            parts = line.split(":")
            if len(parts) >= 4:
                connections.append((parts[0], parts[1], parts[2], parts[3]))
        return connections
    except Exception as e:
        logging.error(f"Failed to query nmcli connections: {e}")
        return []