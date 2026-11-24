"""Count UPS transfers to battery from upsmon's journal.

HA's NUT integration polls each UPS once a minute, so a transfer to battery
that lasts a few seconds usually happens between two polls and HA never sees
it. upsmon polls far more often (POLLFREQ in upsmon.conf) and logs every
transfer, so this collector reads those lines back from the journal:

    UPS <name> on battery
    UPS <name> on line power

These are upsmon's default ONBATT and ONLINE messages; a NOTIFYMSG override
in upsmon.conf would change them. Only transfers that last at least one
upsmon poll are logged.

Config (collectors.ups_events.ups maps upsmon's UPS name to a sensor key):

    collectors:
      ups_events:
        unit: nut-monitor.service       # optional, the default
        ups:
          "ups@10.1.10.181": ups_1
          "apc550@localhost": ups_2

Sensors per UPS key:
    <key>_transfers_10m    transfers in the last 10 minutes; a new transfer
                           shows here for 10 minutes, long enough for HA to
                           see it whatever the publish interval
    <key>_transfers_24h    transfers in the last 24 hours
    <key>_last_transfer    when the last transfer started (timestamp)
    <key>_last_transfer_s  how long it lasted, in seconds (None while still
                           on battery, or if the return was not logged)

The last transfer is looked up over 30 days; older than that, or before the
journal was made persistent, it stays unknown. Needs a persistent journal to
count across a reboot.
"""

import json
import logging
import re
import subprocess
import time
from datetime import datetime, timezone

UPS = {}
UNIT = "nut-monitor.service"
_MSG = re.compile(r"^UPS (\S+) on (battery|line power)$")


def _load(agent):
    global UPS, UNIT
    cfg = (agent.config.get("collectors") or {}).get("ups_events") or {}
    UPS = dict(cfg.get("ups") or {})
    UNIT = cfg.get("unit", UNIT)


# ---------- Discovery ----------


def publish_discovery(agent):
    _load(agent)
    for name, key in UPS.items():
        label = key.replace("_", "-").upper()
        sensors = {
            f"{key}_transfers_10m": {
                "name": f"{label} transfers 10 min", "state_class": "measurement",
                "icon": "mdi:transmission-tower-off"},
            f"{key}_transfers_24h": {
                "name": f"{label} transfers 24 h", "state_class": "measurement",
                "icon": "mdi:transmission-tower-off"},
            f"{key}_last_transfer": {
                "name": f"{label} last transfer", "device_class": "timestamp",
                "icon": "mdi:clock-alert-outline"},
            f"{key}_last_transfer_s": {
                "name": f"{label} last transfer length", "unit_of_measurement": "s",
                "device_class": "duration", "icon": "mdi:timer-outline"},
        }
        for skey, extra in sensors.items():
            topic = f"{agent.discovery_prefix}/sensor/{agent.device}/{skey}/config"
            payload = {
                "state_topic": agent.state_topic,
                "value_template": f"{{{{ value_json.get('{skey}') }}}}",
                "unique_id": f"{agent.device}_{skey}",
                "device": agent.sensor_device_info,
                **extra,
            }
            agent.mqtt.publish(topic, json.dumps(payload), qos=1, retain=True)


# ---------- State collection ----------


def _events(since_s):
    """(time, ups, 'battery'|'line power') for upsmon transfer lines, oldest first."""
    out = subprocess.run(
        ["journalctl", "-u", UNIT, "--since", f"-{since_s}s", "-o", "json",
         "--output-fields=MESSAGE", "--no-pager"],
        capture_output=True, text=True, timeout=20,
    ).stdout
    events = []
    for line in out.splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        msg = rec.get("MESSAGE")
        if not isinstance(msg, str):
            continue
        m = _MSG.match(msg.strip())
        if m:
            events.append((int(rec["__REALTIME_TIMESTAMP"]) / 1e6, m.group(1), m.group(2)))
    return events


def collect(agent):
    if not UPS:
        _load(agent)
    now = time.time()
    try:
        events = _events(30 * 86400)
    except (subprocess.SubprocessError, OSError) as e:
        logging.warning(f"ups_events: journal read failed: {e}")
        return {}

    data = {}
    for name, key in UPS.items():
        mine = [(t, what) for t, ups, what in events if ups == name]
        starts = [t for t, what in mine if what == "battery"]
        data[f"{key}_transfers_10m"] = sum(1 for t in starts if now - t <= 600)
        data[f"{key}_transfers_24h"] = sum(1 for t in starts if now - t <= 86400)
        if starts:
            last = starts[-1]
            back = next((t for t, what in mine if what == "line power" and t >= last), None)
            data[f"{key}_last_transfer"] = datetime.fromtimestamp(last, timezone.utc).isoformat()
            data[f"{key}_last_transfer_s"] = round(back - last) if back else None
    return data
