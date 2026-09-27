"""
rpi_commands.py — RPi-specific command handlers for the upd_control plane.

Each handler receives:
    payload     (dict)      Full command payload from MQTT
    publish_fn  (callable)  Function to publish status messages back to MQTT

Handlers publish status at start and completion so the control plane
has full lifecycle visibility.

COMMAND_REGISTRY maps command names to handler functions.
agent.py imports this registry and dispatches accordingly.
"""

import subprocess
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

# Path to agent_updates.py — same directory as agent.py
SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGENT_UPDATES = os.path.join(SCRIPT_DIR, "agent_updates.py")


# ---------------------------------------------------------------------------
# Status helper
# ---------------------------------------------------------------------------

def _status(publish_fn, payload, status, step, message, extra=None):
    """Build and publish a status message."""
    msg = {
        "host":            payload.get("host", "unknown"),
        "command":         payload.get("action", "unknown"),
        "status":          status,
        "step":            step,
        "message":         message,
        "notification_id": payload.get("notification_id", ""),
        "trace":           payload.get("trace", False),
        "timestamp":       datetime.now().isoformat(),
    }
    if extra:
        msg.update(extra)
    publish_fn(msg)


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def handle_check_updates(payload, publish_fn):
    """
    Trigger an update check by running agent_updates.py.
    Results are published to MQTT by agent_updates.py itself.
    Status messages bookend the run so the control plane knows it started/finished.
    """
    _status(publish_fn, payload, "running", "started", "Update check initiated")

    try:
        result = subprocess.run(
            ["python3", AGENT_UPDATES],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode == 0:
            _status(publish_fn, payload, "success", "complete", "Update check complete")
        else:
            _status(publish_fn, payload, "error", "complete",
                    f"Update check failed: {result.stderr.strip()}")

    except subprocess.TimeoutExpired:
        _status(publish_fn, payload, "error", "complete", "Update check timed out")
    except Exception as e:
        logger.error(f"handle_check_updates error: {e}")
        _status(publish_fn, payload, "error", "complete", str(e))


def handle_run_updates(payload, publish_fn):
    """
    Apply all available updates then run agent_updates.py to verify
    and refresh HA sensors. Two-step so the control plane sees both phases.
    """
    _status(publish_fn, payload, "running", "started", "Update process initiated")

    # Step 1 — apply updates
    try:
        # Refresh apt cache before upgrading
        _status(publish_fn, payload, "running", "applying", "Refreshing apt cache")
        refresh = subprocess.run(
            ["sudo", "apt-get", "update"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if refresh.returncode != 0:
            _status(publish_fn, payload, "error", "complete",
                    f"apt-get update failed: {refresh.stderr.strip()}")
            return

        _status(publish_fn, payload, "running", "applying", "Applying updates via apt-get")
        result = subprocess.run(
            ["sudo", "apt-get", "full-upgrade", "-y"],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode != 0:
            _status(publish_fn, payload, "error", "complete",
                    f"apt-get upgrade failed: {result.stderr.strip()}")
            return

        # Count applied packages from apt output
        applied = sum(
            1 for line in result.stdout.splitlines()
            if line.startswith("Unpacking") or "upgraded" in line.lower()
        )
        logger.info(f"apt-get upgrade completed, ~{applied} operations")

    except subprocess.TimeoutExpired:
        _status(publish_fn, payload, "error", "complete", "apt-get upgrade timed out")
        return
    except Exception as e:
        logger.error(f"handle_run_updates apt error: {e}")
        _status(publish_fn, payload, "error", "complete", str(e))
        return

    # Step 2 — verify via agent_updates.py (also refreshes HA sensors)
    try:
        _status(publish_fn, payload, "running", "verifying", "Running post-update verification")
        verify = subprocess.run(
            ["python3", AGENT_UPDATES],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if verify.returncode == 0:
            _status(publish_fn, payload, "success", "complete",
                    "Updates applied and verified",
                    extra={"updates_applied": applied, "updates_remaining": 0})
        else:
            _status(publish_fn, payload, "error", "complete",
                    f"Verification failed: {verify.stderr.strip()}",
                    extra={"updates_applied": applied})

    except subprocess.TimeoutExpired:
        _status(publish_fn, payload, "error", "complete",
                "Post-update verification timed out",
                extra={"updates_applied": applied})
    except Exception as e:
        logger.error(f"handle_run_updates verify error: {e}")
        _status(publish_fn, payload, "error", "complete", str(e))


def handle_reboot(payload, publish_fn):
    """
    Initiate a system reboot. Status is published before the reboot
    since the Pi won't be able to publish after it.
    """
    _status(publish_fn, payload, "success", "complete", "Reboot initiated")
    logger.info("Reboot command received — rebooting now")

    try:
        subprocess.run(["sudo", "systemctl", "reboot"], check=True)
    except Exception as e:
        logger.error(f"handle_reboot error: {e}")
        _status(publish_fn, payload, "error", "complete", str(e))


def handle_shutdown(payload, publish_fn):
    """
    Initiate a system shutdown. Status is published before shutdown
    since the Pi won't be able to publish after it.
    """
    _status(publish_fn, payload, "success", "complete", "Shutdown initiated")
    logger.info("Shutdown command received — shutting down now")

    try:
        subprocess.run(["sudo", "systemctl", "poweroff"], check=True)
    except Exception as e:
        logger.error(f"handle_shutdown error: {e}")
        _status(publish_fn, payload, "error", "complete", str(e))


# ---------------------------------------------------------------------------
# Registry — agent.py imports this
# ---------------------------------------------------------------------------

COMMAND_REGISTRY = {
    "check":    handle_check_updates,
    "update":   handle_run_updates,
    "reboot":   handle_reboot,
    "shutdown": handle_shutdown,
}