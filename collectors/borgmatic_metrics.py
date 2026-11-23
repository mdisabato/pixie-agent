#!/usr/bin/env python3
"""borgmatic backup sensors, one set per borgmatic job (config file).

Per job (e.g. "hourly", "daily"):
  - borg_<job>_result        : outcome of the job's systemd unit: running,
                               success, failed, or unknown (no run on record)
  - borg_<job>_last_archive  : time of the newest archive the job made
  - borg_<job>_age_hours     : hours since that archive
  - borg_<job>_archives      : how many archives the job has kept
Per repository (shared by the jobs that write to it):
  - borg_repo_size_gb        : space the repository uses on disk
                               (deduplicated and compressed)

Where each value comes from:
  - result comes from systemd (`systemctl show <unit>`): cheap, takes no Borg
    lock, and sees failed runs, which leave no archive behind. systemd forgets
    a unit's last run across a reboot, so for a job that has not run since
    boot the result comes from the journal instead: the last "Deactivated
    successfully" or "Failed with result" message systemd logged for the
    unit. That needs a persistent journal (Storage=persistent in a
    journald.conf.d drop-in; Raspberry Pi OS defaults to volatile). With no
    run in the journal either, the result is "unknown".
  - the archive and size values come from borgmatic itself (`list --json`,
    `repo-info --json`), so the repository passphrase stays in the borgmatic
    config. Those calls open the repository, so they run in a background
    thread, never while a job is running, and with a short lock wait. They
    refresh every `refresh_seconds` (default 900) and right after a job
    finishes. collect() only reads cached values, so a slow repository never
    holds up the agent's main loop.

The newest archive's time is the ground truth for "the last backup that
worked". borg's repository last_modified is not used: pruning and
compacting change it too.

Config (config.yaml):
  collectors:
    borgmatic:
      interval_seconds: 60        # how often result and age are published
      refresh_seconds: 900        # how often the repository is read
      jobs:
        hourly:
          config: /etc/borgmatic.d/hourly.yaml
          unit: borgmatic-hourly.service
        daily:
          config: /etc/borgmatic.d/daily.yaml
          unit: borgmatic-daily.service

The `backup` command in controllers/rpi_commands.py starts these same units,
and only these.
"""

import json
import logging
import subprocess
import threading
import time
from datetime import datetime


LOCK_WAIT_SECONDS = 10      # replaces the job configs' lock_wait for reads
QUERY_TIMEOUT = 300         # seconds for one borgmatic call

# job name -> {"config": path, "unit": systemd unit}; set from config.yaml
JOBS = {}

_cache = {}                 # job -> {"last_archive": datetime, "archives": int}
_repo_size_gb = None
_refresh_cfg = {"seconds": 900}
_last_refresh = 0.0
_last_exit = {}             # unit -> exit timestamp seen on the previous cycle
_journal_results = {}       # unit -> result of its last run before this boot
_refreshing = threading.Lock()

# systemd's catalog IDs for a unit that ended cleanly or failed
_MSG_UNIT_SUCCESS = "7ad2d189f7e94e70a38c781354912448"
_MSG_UNIT_FAILURE = "d9b373ed55a64feb8242e02dbe79a49c"


# ---------- Config ----------


def load_jobs(agent):
    """Fill JOBS from collectors.borgmatic.jobs in config.yaml."""
    cfg = (agent.config.get("collectors") or {}).get("borgmatic") or {}
    JOBS.clear()
    for name, job in (cfg.get("jobs") or {}).items():
        if job and job.get("config") and job.get("unit"):
            JOBS[str(name)] = {"config": job["config"], "unit": job["unit"]}
        else:
            logging.error(f"borgmatic: job {name!r} needs both config and unit; skipped")
    _refresh_cfg["seconds"] = cfg.get("refresh_seconds", 900)


# ---------- Discovery ----------


def publish_discovery(agent):
    load_jobs(agent)
    sensors = {}
    for job in JOBS:
        sensors[f"borg_{job}_result"] = {
            "name": f"Borg {job} result", "icon": "mdi:backup-restore"}
        sensors[f"borg_{job}_last_archive"] = {
            "name": f"Borg {job} last archive", "device_class": "timestamp",
            "icon": "mdi:archive-clock-outline"}
        sensors[f"borg_{job}_age_hours"] = {
            "name": f"Borg {job} age", "unit_of_measurement": "h",
            "state_class": "measurement", "icon": "mdi:timer-sand"}
        sensors[f"borg_{job}_archives"] = {
            "name": f"Borg {job} archives", "state_class": "measurement",
            "icon": "mdi:archive-outline"}
    sensors["borg_repo_size_gb"] = {
        "name": "Borg repository size", "unit_of_measurement": "GB",
        "device_class": "data_size", "state_class": "measurement",
        "icon": "mdi:database-outline"}

    for key, extra in sensors.items():
        topic = f"{agent.discovery_prefix}/sensor/{agent.device}/{key}/config"
        payload = {
            "state_topic": agent.state_topic,
            # A value not read yet is null and renders "None", which HA shows as unknown
            "value_template": f"{{{{ value_json.get('{key}') }}}}",
            "unique_id": f"{agent.device}_{key}",
            "device": agent.sensor_device_info,
            **extra,
        }
        agent.mqtt.publish(topic, json.dumps(payload), qos=1, retain=True)


# ---------- State collection ----------


def collect(agent):
    global _last_refresh
    now = time.time()
    data = {}
    active = False
    just_finished = False

    for job, cfg in JOBS.items():
        props = _unit_props(cfg["unit"])
        result, running = _result_from(props)
        if result == "unknown":
            result = _journal_result(cfg["unit"])
        data[f"borg_{job}_result"] = result
        active = active or running
        # A new exit timestamp means a run ended since the last cycle, even
        # one short enough to start and finish between two cycles.
        exit_stamp = props.get("ExecMainExitTimestampMonotonic", "0")
        if cfg["unit"] in _last_exit and exit_stamp != _last_exit[cfg["unit"]]:
            just_finished = True
        _last_exit[cfg["unit"]] = exit_stamp

        cached = _cache.get(job) or {}
        last = cached.get("last_archive")
        data[f"borg_{job}_last_archive"] = last.isoformat() if last else None
        data[f"borg_{job}_age_hours"] = (
            round((datetime.now().astimezone() - last).total_seconds() / 3600, 2) if last else None)
        data[f"borg_{job}_archives"] = cached.get("archives")
    data["borg_repo_size_gb"] = _repo_size_gb

    # Read the repository when it is due, or as soon as a job has finished,
    # but never while any job is writing to it.
    due = just_finished or not _last_refresh or now - _last_refresh >= _refresh_cfg["seconds"]
    if due and not active and not _refreshing.locked():
        _last_refresh = now
        threading.Thread(target=_refresh, daemon=True).start()

    return data


def unit_result(unit: str):
    """(result, running) for a systemd unit.

    running while the unit is active or starting; success or failed from its
    last run, taken from the journal when it was before this boot; unknown
    when there is no run on record at all.
    """
    result, running = _result_from(_unit_props(unit))
    if result == "unknown":
        result = _journal_result(unit)
    return result, running


def _unit_props(unit: str) -> dict:
    try:
        out = subprocess.run(
            ["systemctl", "show", unit, "-p", "ActiveState",
             "-p", "Result", "-p", "ExecMainExitTimestampMonotonic"],
            capture_output=True, text=True, timeout=5,
        ).stdout
    except (subprocess.SubprocessError, OSError) as e:
        logging.warning(f"borgmatic: systemctl show {unit} failed: {e}")
        return {}
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


def _result_from(props: dict):
    if not props:
        return "unknown", False
    state = props.get("ActiveState", "")
    if state in ("active", "activating", "reloading"):
        return "running", True
    if state == "failed" or props.get("Result", "success") != "success":
        return "failed", False
    if props.get("ExecMainExitTimestampMonotonic", "0") in ("", "0"):
        return "unknown", False
    return "success", False


def _journal_result(unit: str) -> str:
    """success or failed from the unit's last run in the journal, else unknown.

    Only asked while systemd has no run since boot, so the answer cannot
    change until the next run, after which systemd has it again. It is
    looked up once per unit and kept.
    """
    if unit in _journal_results:
        return _journal_results[unit]
    try:
        out = subprocess.run(
            ["journalctl", f"UNIT={unit}",
             f"MESSAGE_ID={_MSG_UNIT_SUCCESS}", f"MESSAGE_ID={_MSG_UNIT_FAILURE}",
             "-n", "1", "-o", "json", "--output-fields=MESSAGE_ID", "--no-pager"],
            capture_output=True, text=True, timeout=15,
        ).stdout.strip()
        msg_id = json.loads(out).get("MESSAGE_ID") if out else None
    except (subprocess.SubprocessError, OSError, ValueError) as e:
        logging.warning(f"borgmatic: journal lookup for {unit} failed: {e}")
        return "unknown"        # not kept, so the next cycle tries again
    result = {_MSG_UNIT_SUCCESS: "success", _MSG_UNIT_FAILURE: "failed"}.get(msg_id, "unknown")
    _journal_results[unit] = result
    logging.info(f"borgmatic: {unit} has no run since boot; journal says {result}")
    return result


# ---------- Repository reads (background thread) ----------


def _refresh():
    global _repo_size_gb
    with _refreshing:
        repos_read = set()
        for job, cfg in JOBS.items():
            archives = _borgmatic(cfg["config"], ["list", "--json"])
            if archives is None:
                continue
            names = archives[0].get("archives", []) if archives else []
            newest = max((a.get("time") or a.get("start") or "" for a in names), default="")
            _cache[job] = {
                "archives": len(names),
                "last_archive": _parse_time(newest),
            }

            repo_id = archives[0].get("repository", {}).get("id") if archives else None
            if repo_id and repo_id not in repos_read:
                info = _borgmatic(cfg["config"], ["repo-info", "--json"])
                if info:
                    stats = info[0].get("cache", {}).get("stats", {})
                    if "unique_csize" in stats:
                        _repo_size_gb = round(stats["unique_csize"] / 1e9, 2)
                        repos_read.add(repo_id)
        logging.info(
            "borgmatic: repository read: "
            + ", ".join(f"{j}={c.get('archives')} archives" for j, c in _cache.items()))


def _borgmatic(config_path: str, action: list):
    """Run one borgmatic action with JSON output; the parsed list, or None."""
    cmd = ["borgmatic", "--config", config_path,
           "--override", f"lock_wait={LOCK_WAIT_SECONDS}", *action]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=QUERY_TIMEOUT)
    except (subprocess.SubprocessError, OSError) as e:
        logging.warning(f"borgmatic: {' '.join(action)} on {config_path} failed: {e}")
        return None
    if result.returncode != 0:
        tail = (result.stderr or "").strip().splitlines()[-1:] or [""]
        logging.warning(f"borgmatic: {' '.join(action)} on {config_path} exited "
                        f"{result.returncode}: {tail[0]}")
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        logging.warning(f"borgmatic: {' '.join(action)} on {config_path} gave no JSON")
        return None


def _parse_time(stamp: str):
    """borg prints local time without an offset; attach the local zone."""
    if not stamp:
        return None
    try:
        return datetime.fromisoformat(stamp).astimezone()
    except ValueError:
        return None
