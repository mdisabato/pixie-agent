import json
import logging
import subprocess
import time

import psutil


# ---------- Config ----------

STALL_THRESHOLD_SEC = 15 * 60          # comfortably above the ~8-min normal
                                        # Modbus exception cycle on Evolution 2.0
ENABLE_SERIAL_STALL_CHECK = False      # *** leave False ***
                                        # A flat read_bytes counter is *expected*
                                        # right now, before the generator is
                                        # physically wired up -- genmon correctly
                                        # reports "not receiving data" in that
                                        # state, and this check can't tell that
                                        # apart from a real wedge from the
                                        # outside. Flip to True only once genmon
                                        # is confirmed parsing live data from the
                                        # real generator (a real baseline to
                                        # fall from, not a permanent flat line).
RESTART_COOLDOWN_SEC = 20 * 60         # don't re-trigger a restart more often
                                        # than this, even if conditions persist
RESTART_UNIT = "genmon.service"
WATCHED_PROCESSES = ("genmon.py", "genserv.py", "genhalink.py")

# In-module state -- unlike its sibling collectors, this one needs to
# remember something between cycles (last serial-activity timestamp, last
# restart time for the cooldown). Kept as plain globals rather than a state
# file: pixie-agent is one long-running process, so this survives fine for
# the process's lifetime, and losing the baseline on a rare pixie-agent
# restart is an acceptable edge case -- same trade-off as everything else
# in this agent, none of which persists to disk either.
_last_read_bytes = None
_last_activity_ts = time.time()
_last_restart_ts = 0.0


# ---------- Discovery ----------


def publish_discovery(agent):
    """Two binary sensors, sharing service_metrics' state topic/device group
    (Services), since this is fundamentally the same kind of thing: genmon's
    actual health, one level deeper than the plain systemd is-active check
    the 'genmon' entry in config.yaml's services: list already gives you.
    """
    sensors = {
        "genmon_process_ok": "Genmon Process OK",
        "genmon_serial_active": "Genmon Serial Active",
    }
    for key, name in sensors.items():
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
            "payload_on": "ON",
            "payload_off": "OFF",
            "icon": "mdi:engine-outline",
        }
        agent.mqtt.publish(topic, json.dumps(payload), retain=True)


# ---------- State collection ----------


def collect(agent):
    """Checks genmon.py/genserv.py/genhalink.py liveness (always) and serial
    read activity (only if ENABLE_SERIAL_STALL_CHECK is True). Restarts
    genmon.service on either failure -- this restarts only the monitoring
    software on the Pi, never the generator, transfer switch, or any control
    relay, consistent with keeping generator start/stop/transfer strictly
    manual and dashboard-confirmed.
    """
    global _last_read_bytes, _last_activity_ts

    missing = [name for name in WATCHED_PROCESSES if _find_proc(name) is None]
    process_ok = not missing
    if missing:
        logging.warning(f"genmon-watchdog: not running: {', '.join(missing)}")

    serial_active = True
    if ENABLE_SERIAL_STALL_CHECK:
        genmon_proc = _find_proc("genmon.py")
        if genmon_proc is not None:
            rb = _read_bytes_for(genmon_proc.pid)
            if rb is not None:
                if rb != _last_read_bytes:
                    _last_read_bytes = rb
                    _last_activity_ts = time.time()
                stalled_for = time.time() - _last_activity_ts
                serial_active = stalled_for < STALL_THRESHOLD_SEC
                if not serial_active:
                    logging.warning(
                        f"genmon-watchdog: no serial read activity for {stalled_for:.0f}s"
                    )

    if not process_ok:
        _restart_genmon(f"not running: {', '.join(missing)}")
    elif not serial_active:
        _restart_genmon(f"no serial I/O for over {STALL_THRESHOLD_SEC}s")

    return {
        "genmon_process_ok": "ON" if process_ok else "OFF",
        "genmon_serial_active": "ON" if serial_active else "OFF",
    }


# ---------- Helpers ----------


def _find_proc(match: str):
    for p in psutil.process_iter(["pid", "cmdline"]):
        try:
            if any(match in part for part in (p.info["cmdline"] or [])):
                return p
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def _read_bytes_for(pid: int):
    try:
        with open(f"/proc/{pid}/io") as f:
            for line in f:
                if line.startswith("read_bytes:"):
                    return int(line.split(":")[1].strip())
    except (FileNotFoundError, PermissionError):
        return None
    return None


def _restart_genmon(reason: str) -> None:
    global _last_restart_ts
    now = time.time()
    if now - _last_restart_ts < RESTART_COOLDOWN_SEC:
        logging.warning(f"genmon-watchdog: suppressing restart (cooldown active): {reason}")
        return
    logging.warning(f"genmon-watchdog: restarting {RESTART_UNIT}: {reason}")
    subprocess.run(["systemctl", "restart", RESTART_UNIT], check=False)
    _last_restart_ts = now
