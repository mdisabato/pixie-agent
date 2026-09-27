#!/usr/bin/env python3

import json
import time
import signal
import logging
import os
import subprocess
import ssl
import sys
import threading

import yaml
import paho.mqtt.client as mqtt

from collectors import protonvpn_metrics
from collectors import system_metrics
from collectors import disk_metrics
from collectors import service_metrics
from controllers.rpi_commands import COMMAND_REGISTRY


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class Agent:
    def __init__(self, config):
        self._connected = threading.Event()
        self.config = config
        self.device = config["device"]["name"]
        self.collectors = self._collector_schedule(config)

        # Latest values per topic, merged across collectors
        self._state = {}
        self._service_state = {}

        # runtime state
        self.running = False
        self.start_time = time.time()

        # Home Assistant discovery
        self.discovery_prefix = config.get("discovery_prefix", "homeassistant")

        # Device info per collector group
        self.sensor_device_info = {
            "identifiers": [f"{self.device}_sensor"],
            "name": f"{self.device} Sensors",
            "model": config["device"].get("model", "Raspberry Pi"),
            "manufacturer": config["device"].get("manufacturer", "Raspberry Pi Foundation"),
        }

        self.service_device_info = {
            "identifiers": [f"{self.device}_service"],
            "name": f"{self.device} Services",
            "model": config["device"].get("model", "Raspberry Pi"),
            "manufacturer": config["device"].get("manufacturer", "pixie-agent"),
        }

        # Keep a default for backward compatibility
        self.device_info = self.sensor_device_info

        # MQTT topics — monitoring
        base_topic = config["mqtt"].get("base_topic", "system-sensors/sensor")
        self.state_topic = f"{base_topic}/{self.device}/state"
        self.status_topic = f"{base_topic}/{self.device}/status"

        service_base = config["mqtt"].get("service_base_topic", "system-services/sensor")
        self.service_state_topic = f"{service_base}/{self.device}/state"

        # MQTT topics — control plane
        self.command_topic = f"upd_control/{self.device}/command"
        self.command_status_topic = f"upd_control/{self.device}/status"

        # MQTT setup
        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt.username_pw_set(
            config["mqtt"]["username"],
            config["mqtt"]["password"],
        )

        # TLS setup if configured
        tls_config = config.get("mqtt", {}).get("tls", {})
        if tls_config:
            self.mqtt.tls_set(
                ca_certs=tls_config["ca_cert"],
                tls_version=ssl.PROTOCOL_TLS_CLIENT,
            )

        # Last Will and Testament
        self.mqtt.will_set(
            self.status_topic,
            json.dumps({"action": "unexshutdown"}),
            qos=1,
            retain=True,
        )

        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_message = self._on_message

    # ---------- Utility ----------

    def json(self, payload):
        return json.dumps(payload, separators=(",", ":"))

    # ---------- MQTT callbacks ----------

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            logging.error(f"MQTT connection failed: {reason_code}")
            return
        self._connected.set()

        logging.info("MQTT connected")

        # Subscribe to control plane commands
        self.mqtt.subscribe(self.command_topic)
        logging.info(f"Subscribed to {self.command_topic}")

        # Publish Home Assistant discovery
        system_metrics.publish_discovery(self)
        logging.info("system discovery published")

        disk_metrics.publish_discovery(self)
        logging.info("disk discovery published")

        service_metrics.publish_discovery(self)
        logging.info("service discovery published")

        protonvpn_metrics.publish_discovery(self)
        logging.info("protonvpn discovery published")

        self._publish_status_discovery()
        logging.info("status discovery published")

        self._publish_command_status_discovery()
        logging.info("command status discovery published")

        # Publish ready state
        self._publish_status("ready")
        logging.info("status: ready")

        # Publish online status to control plane — clears any prior command state
        # and notifies Node-RED the host is back after reboot/shutdown
        from datetime import datetime
        self.publish_command_status({
            "host":            self.device,
            "command":         "ready",
            "status":          "ready",
            "step":            "online",
            "message":         "Agent online",
            "notification_id": "",
            "trace":           False,
            "timestamp":       datetime.now().isoformat(),
        })
        logging.info("command status: online")

    def _on_message(self, client, userdata, msg):
        """Receive command message and dispatch to registry handler in a thread."""
        try:
            payload = json.loads(msg.payload.decode())
            action = payload.get("action", "").lower()
            logging.info(f"Command received: {action} from {payload.get('host', 'unknown')}")

            handler = COMMAND_REGISTRY.get(action)
            if handler:
                # Run in thread — handlers can be long-running (apt-get, etc.)
                thread = threading.Thread(
                    target=handler,
                    args=(payload, self.publish_command_status),
                    daemon=True,
                )
                thread.start()
            else:
                logging.warning(f"Unknown command: {action}")
                self.publish_command_status({
                    "host":            payload.get("host", self.device),
                    "command":         action,
                    "status":          "error",
                    "step":            "complete",
                    "message":         f"Unknown command: {action}",
                    "notification_id": payload.get("notification_id", ""),
                    "trace":           payload.get("trace", False),
                })

        except json.JSONDecodeError:
            logging.error(f"Invalid JSON in command message: {msg.payload.decode()}")
        except Exception as e:
            logging.error(f"Command handling failed: {e}")

    # ---------- Command status ----------

    def publish_command_status(self, status_payload: dict) -> None:
        """Publish command status back to the control plane topic."""
        try:
            self.mqtt.publish(
                self.command_status_topic,
                json.dumps(status_payload),
                qos=1,
                retain=True,
            )
            logging.info(
                f"Command status published: {status_payload.get('command')} "
                f"[{status_payload.get('status')}] {status_payload.get('step')}"
            )
        except Exception as e:
            logging.error(f"Failed to publish command status: {e}")

    # ---------- Agent Status ----------

    def _publish_status_discovery(self) -> None:
        """Publish HA discovery for agent status sensor."""
        topic = (
            f"{self.discovery_prefix}/sensor/"
            f"{self.device}/agent_status/config"
        )

        payload = {
            "name": "Agent Status",
            "state_topic": self.status_topic,
            "value_template": "{{ value_json.action }}",
            "unique_id": f"{self.device}_agent_status",
            "device": self.service_device_info,
            "icon": "mdi:raspberry-pi",
        }

        self.mqtt.publish(topic, json.dumps(payload), qos=1, retain=True)

    def _publish_command_status_discovery(self) -> None:
        """Publish HA discovery for four control plane sensors.

        All four sensors read from the same retained topic so there is
        no additional MQTT traffic — the broker delivers one message and
        HA updates four entity states simultaneously.
        """
        sensors = {
            "command_cmd": {
                "name": "Command",
                "value_template": "{{ value_json.command }}",
                "icon": "mdi:console",
            },
            "command_status": {
                "name": "Command Status",
                "value_template": "{{ value_json.status }}",
                "icon": "mdi:check-circle-outline",
            },
            "command_step": {
                "name": "Command Step",
                "value_template": "{{ value_json.step }}",
                "icon": "mdi:stairs",
            },
            "command_message": {
                "name": "Command Message",
                "value_template": "{{ value_json.message }}",
                "icon": "mdi:message-text-outline",
            },
        }

        for unique_suffix, sensor in sensors.items():
            topic = (
                f"{self.discovery_prefix}/sensor/"
                f"{self.device}/{unique_suffix}/config"
            )
            payload = {
                "name": sensor["name"],
                "state_topic": self.command_status_topic,
                "value_template": sensor["value_template"],
                "unique_id": f"{self.device}_{unique_suffix}",
                "device": self.service_device_info,
                "icon": sensor["icon"],
            }
            self.mqtt.publish(topic, json.dumps(payload), qos=1, retain=True)

    def _publish_status(self, state: str) -> None:
        """Publish agent lifecycle status message."""
        payload = json.dumps({"action": state})
        self.mqtt.publish(self.status_topic, payload, qos=1, retain=True)

    def _check_system_state(self) -> str:
        """Detect if system is rebooting or shutting down."""
        try:
            result = subprocess.run(
                ["systemctl", "list-jobs"],
                capture_output=True,
                text=True,
                timeout=2,
            )
            output = result.stdout.lower()

            if "reboot.target" in output:
                return "reboot"
            if "shutdown.target" in output or "poweroff.target" in output:
                return "shutdown"

            if os.path.exists("/run/systemd/reboot-mode"):
                return "reboot"

            result = subprocess.run(
                ["systemctl", "is-system-running"],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if "stopping" in result.stdout.lower():
                return "shutdown"

        except (subprocess.TimeoutExpired, subprocess.SubprocessError, OSError) as e:
            logging.warning(f"Error checking system state: {e}")

        return "shutdown"

    # ---------- Collection ----------

    def _collector_schedule(self, config):
        """Build (name, module, topic, interval_seconds) for each collector.

        Intervals come from collectors.<name>.interval_seconds in config.yaml,
        falling back to the top-level "interval" (default 60). protonvpn is
        published alongside the system metrics, so it defaults to the system
        interval.
        """
        default = config.get("interval", 60)
        collectors_cfg = config.get("collectors") or {}

        def interval(name, fallback):
            return (collectors_cfg.get(name) or {}).get("interval_seconds", fallback)

        system_interval = interval("system", default)
        return [
            ("system",    system_metrics,    "state",    system_interval),
            ("disk",      disk_metrics,      "state",    interval("disk", default)),
            ("protonvpn", protonvpn_metrics, "state",    interval("protonvpn", system_interval)),
            ("services",  service_metrics,   "services", interval("services", default)),
        ]

    def _run_collectors(self, due):
        """Run the due collectors and publish the topics they feed.

        Results are merged into cached payloads, so a publish always carries
        the latest value of every metric — including those from collectors
        that weren't due this time.
        """
        cycle_start = time.time()
        ran = set()

        for name, module, topic, _ in due:
            try:
                data = module.collect(self)
            except Exception as e:
                logging.error(f"{name} collection failed: {e}")
                continue
            if topic == "state":
                self._state.update(data)
            else:
                self._service_state = data
            ran.add(topic)

        try:
            if "state" in ran:
                self._state["agent_cycle_ms"] = round((time.time() - cycle_start) * 1000, 1)
                self.mqtt.publish(self.state_topic, self.json(self._state), retain=True)

            # Service metrics on separate topic
            if "services" in ran:
                self.mqtt.publish(
                    self.service_state_topic,
                    self.json(self._service_state),
                    retain=True,
                )

            logging.debug(f"published {sorted(ran)} ({len(self._state)} metrics)")
        except Exception as e:
            logging.error(f"Publish failed: {e}")

    # ---------- Signal handling ----------

    def _handle_signal(self, signum, frame):
        logging.info("Shutdown signal received")
        self.running = False

    # ---------- Main loop ----------

    def run(self):
        # Install signal handlers (only valid in main thread)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        # Connect to MQTT broker
        try:
            self.mqtt.connect(
                self.config["mqtt"]["broker"],
                self.config["mqtt"].get("port", 1883),
                60,
            )
        except Exception as e:
            logging.error(f"MQTT connect failed: {e}")
            sys.exit(1)  # non-zero so systemd Restart=on-failure triggers

        self.mqtt.loop_start()
        # Wait up to 10 seconds for connection to establish
        if not self._connected.wait(timeout=10):
            logging.error("MQTT connection timeout")
            sys.exit(1)

        self.running = True

        logging.info(
            "collector intervals: "
            + ", ".join(f"{name}={interval}s" for name, _, _, interval in self.collectors)
        )

        # Every collector is due immediately so the first publish is complete
        next_run = {name: 0.0 for name, _, _, _ in self.collectors}

        while self.running:
            now = time.monotonic()
            due = [c for c in self.collectors if now >= next_run[c[0]]]

            if due:
                self._run_collectors(due)
                for name, _, _, interval in due:
                    next_run[name] = now + interval

            # Interruptible sleep until the next collector is due —
            # exits within 1 second of SIGTERM
            wait = min(next_run.values()) - time.monotonic()
            while self.running and wait > 0:
                time.sleep(min(1.0, wait))
                wait -= 1.0

        # Graceful shutdown — detect why and publish final status
        state = self._check_system_state()
        logging.info(f"Publishing shutdown state: {state}")
        self._publish_status(state)

        # Publish offline step to command status topic
        from datetime import datetime
        self.publish_command_status({
            "host":            self.device,
            "command":         state,
            "status":          "offline",
            "step":            "offline",
            "message":         f"Agent going offline — {state}",
            "notification_id": "",
            "trace":           False,
            "timestamp":       datetime.now().isoformat(),
        })
        logging.info("command status: offline")
        time.sleep(0.5)  # give MQTT time to deliver

        self.mqtt.loop_stop()
        time.sleep(0.5)  # final drain before disconnect
        self.mqtt.disconnect()
        logging.info("Agent stopped")


# ---------- Entry ----------


def load_config(path=None):
    if path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(script_dir, 'config.yaml')
    with open(path, "r") as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    config = load_config()
    agent = Agent(config)
    agent.run()