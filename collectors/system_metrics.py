import logging
import json
import platform
import socket
import subprocess
import time

import psutil

logger = logging.getLogger(__name__)

# --- Network rate state (equivalent to old get_net_data) ---
_last_net_rx = None
_last_net_tx = None
_last_time_rx = None
_last_time_tx = None


def _run_vcgencmd(*args: str) -> str | None:
    """Run vcgencmd and return stdout as a stripped string, or None on error."""
    try:
        result = subprocess.run(
            ["vcgencmd", *args],
            capture_output=True,
            text=True,
            check=True,  # raise CalledProcessError on non‑zero exit
            timeout=2,   # a hung vcgencmd must not stall the collection loop
        )
        return result.stdout.strip()
    except Exception:
        return None


def _parse_vcgencmd_temp(out: str | None) -> float | None:
    """
    Parse vcgencmd measure_temp output like "temp=49.8'C" into a float (e.g. 49.8).
    Returns None on failure.
    """
    if not out:
        return None
    try:
        # "temp=49.8'C" -> "49.8"
        value = out.split("=")[1].split("'")[0]
        return float(value)
    except Exception:
        return None


# ---------- Home Assistant Discovery ----------


def publish_discovery(agent) -> None:
    sensors = {
        # CPU / system
        "cpu_temp":       ("CPU Temperature", "°C", "temperature", "measurement", "thermometer"),
        "cpu_usage":      ("CPU Usage", "%", None, "measurement", "chip"),
        "clock_speed":    ("Clock Speed", "MHz", None, "measurement", "speedometer"),
        "load_1m":        ("Load 1m", None, None, "measurement", "cpu-64-bit"),
        "load_5m":        ("Load 5m", None, None, "measurement", "cpu-64-bit"),
        "load_15m":       ("Load 15m", None, None, "measurement", "cpu-64-bit"),

        # Memory
        "memory_use":     ("Memory Use", "%", None, "measurement", "memory"),
        "swap_usage":     ("Swap Usage", "%", None, "measurement", "harddisk"),

        # System identity
        "hostname":       ("Hostname", None, None, None, "identifier"),
        "host_os":        ("Operating System", None, None, None, "linux"),
        "host_arch":      ("Architecture", None, None, None, "chip"),
        "host_ip":        ("IP Address", None, None, None, "ip-network"),

        # Network (Kbps, rate)
        "net_rx":         ("Network Download", "Kbps", None, "measurement", "server-network"),
        "net_tx":         ("Network Upload", "Kbps", None, "measurement", "server-network"),

        # WiFi
        "wifi_ssid":      ("WiFi SSID", None, None, None, "wifi"),
        "wifi_strength":  ("WiFi Signal", "dBm", "signal_strength", "measurement", "wifi"),

        # Hardware temps
        "pmic_temp":      ("PMIC Temperature", "°C", "temperature", "measurement", "thermometer"),
        "fan_speed":      ("Fan Speed", "RPM", None, "measurement", "fan"),

        # Disk (GiB)
        "disk_total_gb":  ("Disk Total", "GB", None, "measurement", "harddisk"),
        "disk_used_gb":   ("Disk Used", "GB", None, "measurement", "harddisk"),
        "disk_free_gb":   ("Disk Free", "GB", None, "measurement", "harddisk"),
        "disk_use":       ("Disk Usage", "%", None, "measurement", "harddisk"),

        # System
        "last_boot":      ("Last Boot", None, "timestamp", None, "clock"),
    }

    binary_sensors = {
        "power_status": ("Under Voltage", "problem", "alert"),
    }

    for key, (name, unit, device_class, state_class, icon) in sensors.items():
        _publish_sensor(agent, key, name, unit, device_class, state_class, icon)

    for key, (name, device_class, icon) in binary_sensors.items():
        _publish_binary_sensor(agent, key, name, device_class, icon)


def _publish_sensor(agent, key, name, unit, device_class, state_class, icon) -> None:
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
    }

    if unit:
        payload["unit_of_measurement"] = unit
    if device_class:
        payload["device_class"] = device_class
    if state_class:
        payload["state_class"] = state_class
    if icon:
        payload["icon"] = f"mdi:{icon}"

    agent.mqtt.publish(topic, json.dumps(payload), retain=True)


def _publish_binary_sensor(agent, key, name, device_class, icon) -> None:
    topic = (
        f"{agent.discovery_prefix}/binary_sensor/"
        f"{agent.device}/{key}/config"
    )

    payload = {
        "name": name,
        "state_topic": agent.state_topic,
        "value_template": f"{{{{ value_json.{key} }}}}",
        "unique_id": f"{agent.device}_{key}",
        "device": agent.device_info,
    }

    if device_class:
        payload["device_class"] = device_class
    if icon:
        payload["icon"] = f"mdi:{icon}"

    agent.mqtt.publish(topic, json.dumps(payload), retain=True)


# ---------- Data Collection ----------


def collect(agent) -> dict:
    data: dict[str, object] = {}

    # CPU
    data["cpu_usage"] = psutil.cpu_percent()
    freq = psutil.cpu_freq()
    data["clock_speed"] = int(freq.current) if freq else None
    data["load_1m"], data["load_5m"], data["load_15m"] = psutil.getloadavg()

    # Memory
    vm = psutil.virtual_memory()
    data["memory_use"] = vm.percent
    data["swap_usage"] = psutil.swap_memory().percent

    # Identity
    hostname = socket.gethostname()
    data["hostname"] = hostname
    data["host_os"] = platform.system()
    data["host_arch"] = platform.machine()
    try:
        data["host_ip"] = socket.gethostbyname(hostname)
    except Exception:
        data["host_ip"] = None

    # Network rates (Kbps), equivalent to old get_net_data
    data["net_rx"] = round(_get_net_rate(is_tx=False), 2)
    data["net_tx"] = round(_get_net_rate(is_tx=True), 2)

    # Disk (root filesystem) in bytes + GiB
    disk = psutil.disk_usage("/")
    gib = 1024 ** 3
    data["disk_total_bytes"] = disk.total
    data["disk_used_bytes"] = disk.used
    data["disk_free_bytes"] = disk.free
    data["disk_total_gb"] = round(disk.total / gib, 2)
    data["disk_used_gb"] = round(disk.used / gib, 2)
    data["disk_free_gb"] = round(disk.free / gib, 2)
    data["disk_use"] = disk.percent

    # Temperatures (Raspberry Pi)
    data["cpu_temp"] = _read_cpu_temp()
    data["pmic_temp"] = _read_pmic_temp()

    # WiFi
    ssid, strength = _read_wifi_ssid_and_signal()
    data["wifi_ssid"] = ssid
    data["wifi_strength"] = strength

    # Fan (no hardware → None)
    data["fan_speed"] = _read_fan_speed()

    # Undervoltage / power status (binary_sensor expects ON/OFF)
    data["power_status"] = "ON" if _get_rpi_undervoltage() else "OFF"

    # Agent uptime
    data["agent_uptime"] = int(time.time() - agent.start_time)

    # Boot time (ISO 8601 timestamp)
    from datetime import datetime, timezone
    data["last_boot"] = datetime.fromtimestamp(
        psutil.boot_time(), tz=timezone.utc
    ).isoformat()

    return data


# ---------- Network helpers (rate) ----------


def _get_net_rate(is_tx: bool) -> float:
    """
    Return network rate in Kbps, similar to old get_net_data().
    Uses psutil.net_io_counters() and internal previous values.
    """
    global _last_net_rx, _last_net_tx, _last_time_rx, _last_time_tx

    counters = psutil.net_io_counters()
    now = time.time()

    if is_tx:
        current_data = counters.bytes_sent
        previous_data = _last_net_tx
        previous_time = _last_time_tx
        _last_net_tx = current_data
        _last_time_tx = now
    else:
        current_data = counters.bytes_recv
        previous_data = _last_net_rx
        previous_time = _last_time_rx
        _last_net_rx = current_data
        _last_time_rx = now

    # First call: no previous data, return 0
    if previous_data is None or previous_time is None:
        return 0.0

    if now == previous_time:
        now += 1.0

    net_data_kbps = (current_data - previous_data) * 8.0 / (now - previous_time) / 1024.0
    return max(net_data_kbps, 0.0)


# ---------- WiFi ----------


def _read_wifi_ssid_and_signal() -> tuple[str, int | str]:
    """
    Read WiFi SSID and signal strength using iwconfig.
    Returns (ssid, strength_dbm).
    Returns ("N/A", None) if no wireless interface is active.
    Returns ("unknown", None) on error.
    """
    try:
        # Check if any wireless interface exists
        import pathlib
        wireless_interfaces = list(pathlib.Path("/sys/class/net").glob("wlan*"))
        if not wireless_interfaces:
            return "N/A", "N/A"

        # Check if the interface is up
        iface = wireless_interfaces[0].name
        operstate = pathlib.Path(f"/sys/class/net/{iface}/operstate").read_text().strip()
        if operstate != "up":
            return "N/A", "N/A"

        result = subprocess.run(
            ["iwconfig", iface],
            capture_output=True,
            text=True,
            timeout=2,
        ).stdout

        import re

        ssid_match = re.search(r'ESSID:"([^"]+)"', result)
        signal_match = re.search(r"Signal level=(-?\d+)", result)

        ssid = ssid_match.group(1) if ssid_match else "N/A"
        strength = int(signal_match.group(1)) if signal_match else "N/A"
        return ssid, strength
    except Exception:
        return "unknown", "N/A"


def _read_fan_speed() -> int | None:
    """Raspberry Pi 4 with no dedicated fan tachometer hardware."""
    return None


# ---------- Hardware Helpers ----------


def _read_cpu_temp() -> float | None:
    # Works on Raspberry Pi OS
    return _parse_vcgencmd_temp(_run_vcgencmd("measure_temp"))


def _read_pmic_temp() -> float | None:
    # Works on Raspberry Pi OS
    return _parse_vcgencmd_temp(_run_vcgencmd("measure_temp", "pmic"))


def _read_clock_speed() -> int | None:
    # Works on Raspberry Pi OS
    out = _run_vcgencmd("measure_clock", "arm")
    if not out:
        return None
    try:
        # output looks like: "frequency(48)=1500000000"
        hz = int(out.split("=")[1])
        return int(hz / 1_000_000)
    except Exception:
        return None


def _get_rpi_undervoltage() -> bool:
    out = _run_vcgencmd("get_throttled")
    if not out:
        return False
    try:
        # output looks like: "throttled=0x0"
        value = int(out.split("=")[1], 16)
        return bool(value & 0x1)
    except Exception:
        return False