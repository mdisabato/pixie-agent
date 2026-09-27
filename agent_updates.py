import json
import subprocess
import ssl
import paho.mqtt.client as mqtt
import logging
from logging.handlers import RotatingFileHandler
import os
import yaml
import socket
from datetime import datetime
import time
import pytz

# Load config.yaml from same directory as this script
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'config.yaml')

with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# MQTT from shared config
mqtt_cfg = config["mqtt"]
MQTT_BROKER = mqtt_cfg["broker"]
MQTT_PORT = mqtt_cfg.get("port", 1883)
MQTT_USERNAME = mqtt_cfg.get("username")
MQTT_PASSWORD = mqtt_cfg.get("password")
MQTT_QOS = 1
MQTT_RETAIN = True
MQTT_RECONNECT_DELAY = 5

# Device / naming
DEVICE_NAME = config["device"]["name"]

# Discovery / topic prefixes
MQTT_TOPIC_PREFIX = "system-updates"
DISCOVERY_TOPIC = config.get("homeassistant", {}).get("discovery_prefix", "homeassistant")
DEVICE_UNIQUE_ID = DEVICE_NAME

# System Constants
APT_LIST_CMD = "apt list --upgradable 2>/dev/null"
REBOOT_FILE_PATH = '/var/run/reboot-required'
APT_HISTORY_LOG = '/var/log/apt/history.log'

# Update-check settings (optional "updates:" section of config.yaml)
updates_cfg = config.get("updates", {})

# Timezone used for timestamps (e.g., 'America/New_York')
timezone = pytz.timezone(updates_cfg.get("timezone", "UTC"))
now = datetime.now(timezone)

# Get the hostname of the machine
hostname = socket.gethostname()

# Define the log file path using the hostname
LOG_DIR = updates_cfg.get("log_dir", "/var/log/pixie-agent")
LOG_FILE_PATH = os.path.join(LOG_DIR, f'{hostname}.chk_updates.log')
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

# Setup logging globally
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(LOG_FILE_PATH, maxBytes=1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info('Script started')

# Get the current script file path
script_path = os.path.realpath(__file__)

# Get the last modification time of the script
modification_time = os.path.getmtime(script_path)
modification_time_dt = datetime.fromtimestamp(modification_time, tz=timezone)
script_version = modification_time_dt.strftime('%Y-%m-%d %H:%M:%S %Z')

# Log script version and start time
logger.info(f'Script version: {script_version}')

# MQTT defaults
LOG_LEVEL = getattr(logging, config.get('log_level', 'INFO').upper(), logging.INFO)
logger.setLevel(LOG_LEVEL)

# Add a flag to ensure connection is complete before publishing
connected_flag = False


# ---------------------------------------------------------------------------
# MQTT callbacks — defined before client setup so assignments don't fail
# ---------------------------------------------------------------------------

def on_connect(client, userdata, flags, reason_code, properties):
    """Callback for when MQTT client connects to broker."""
    global connected_flag
    if reason_code == 0:
        logger.info("Connected to MQTT Broker!")
        connected_flag = True
    else:
        logger.error(f"Failed to connect, return code {reason_code}")

def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback for when MQTT client disconnects from broker."""
    global connected_flag
    connected_flag = False
    if reason_code != 0:
        logger.warning(f"Unexpected disconnect (code: {reason_code}). Attempting reconnect...")
        while not connected_flag:
            try:
                client.reconnect()
                time.sleep(MQTT_RECONNECT_DELAY)
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")


# ---------------------------------------------------------------------------
# MQTT client setup
# ---------------------------------------------------------------------------

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

# TLS setup if configured
tls_config = mqtt_cfg.get("tls", {})
if tls_config:
    client.tls_set(
        ca_certs=tls_config["ca_cert"],
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )
    logger.info("TLS enabled")

client.on_connect = on_connect
client.on_disconnect = on_disconnect


# ---------------------------------------------------------------------------
# APT helpers
# ---------------------------------------------------------------------------

def update_apt_cache():
    """Update APT package cache."""
    try:
        result = subprocess.run(["apt", "update"], capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"apt update failed: {result.stderr}")
            raise Exception(f"apt update failed with return code {result.returncode}")
        logger.info("APT cache updated successfully")
    except Exception as e:
        logger.error(f"Error updating APT cache: {e}")
        raise

def clean_apt_cache():
    """Clean APT package cache."""
    try:
        result = subprocess.run(["apt", "clean"], capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"apt clean failed: {result.stderr}")
            raise Exception(f"apt clean failed with return code {result.returncode}")
        logger.info("APT cache cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning APT cache: {e}")


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_sensors():
    """Publish Home Assistant MQTT discovery configurations for all sensors."""

    sensors = {
        'security_updates': {
            'name': 'Security Updates',
            'icon': 'mdi:shield-alert',
            'unique_id': f'{hostname}_security_updates',
            'state_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/security_updates/state',
            'value_template': '{{ value_json.security_updates }}',
            'json_attributes_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/security_updates/attributes',
            'json_attributes_template': '{{ value_json.update_details | tojson }}'
        },
        'software_updates': {
            'name': 'Software Updates',
            'icon': 'mdi:update',
            'unique_id': f'{hostname}_software_updates',
            'state_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/software_updates/state',
            'value_template': '{{ value_json.software_updates }}',
            'json_attributes_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/software_updates/attributes',
            'json_attributes_template': '{{ value_json.update_details | tojson }}'
        },
        'reboot_needed': {
            'name': 'Reboot Needed',
            'icon': 'mdi:restart-alert',
            'unique_id': f'{hostname}_reboot_needed',
            'state_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/reboot_needed/state',
            'value_template': '{{ value_json.reboot_needed }}'
        },
        'last_updated': {
            'name': 'Last Updated',
            'icon': 'mdi:clipboard-text-clock-outline',
            'unique_id': f'{hostname}_last_updated',
            'device_class': 'timestamp',
            'state_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/last_updated/state',
            'value_template': '{{ value_json.last_updated }}'
        },
        'last_checked': {
            'name': 'Last Checked',
            'icon': 'mdi:clipboard-text-clock-outline',
            'unique_id': f'{hostname}_last_checked',
            'device_class': 'timestamp',
            'state_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/last_checked/state',
            'value_template': '{{ value_json.last_checked }}'
        },
        'os_name': {
            'name': 'OS Name',
            'icon': 'mdi:debian',
            'unique_id': f'{hostname}_os_name',
            'state_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/os_name/state',
            'value_template': '{{ value_json.os_name }}'
        },
        'os_codename': {
            'name': 'OS Codename',
            'icon': 'mdi:debian',
            'unique_id': f'{hostname}_os_codename',
            'state_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/os_codename/state',
            'value_template': '{{ value_json.os_codename }}'
        },
        'os_version': {
            'name': 'OS Version',
            'icon': 'mdi:debian',
            'unique_id': f'{hostname}_os_version',
            'state_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/os_version/state',
            'value_template': '{{ value_json.os_version }}'
        },
        'os_date': {
            'name': 'OS Release Date',
            'icon': 'mdi:debian',
            'unique_id': f'{hostname}_os_date',
            'state_topic': f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/os_date/state',
            'value_template': '{{ value_json.os_date }}'
        }
    }

    logger.info('Preparing configuration payloads')

    for sensor, cfg in sensors.items():
        payload = {
            'name': cfg['name'],
            'state_topic': cfg['state_topic'],
            'value_template': cfg['value_template'],
            'icon': cfg['icon'],
            'unique_id': cfg['unique_id'],
            'device': {
                'identifiers': [f"rpi4_{DEVICE_UNIQUE_ID}"],
                'name': f"{hostname}",
                'model': 'Debian GNU/Linux',
                'manufacturer': 'Debian'
            }
        }
        if 'device_class' in cfg:
            payload['device_class'] = cfg['device_class']

        if sensor in ['security_updates', 'software_updates']:
            payload.update({
                'json_attributes_topic': cfg.get('json_attributes_topic'),
            })

        logger.info(f'Published discovery message for {sensor}')
        mqtt_publish(f'{DISCOVERY_TOPIC}/sensor/{hostname}/{sensor}/config', json.dumps(payload))


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

def get_last_updated_time():
    """Get timestamp of last successful apt update."""
    try:
        with open(APT_HISTORY_LOG, 'r') as f:
            for line in reversed(f.readlines()):
                if line.startswith("End-Date:"):
                    date_str = line[len("End-Date:"):].strip()
                    try:
                        update_time = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                        return timezone.localize(update_time).isoformat()
                    except ValueError as ve:
                        logger.error(f"Failed to parse date: {ve}")
                        return now.strftime('%Y-%m-%dT%H:%M:%S%z')
    except Exception as e:
        logger.error(f"Failed to get last updated time: {e}")
        return now.strftime('%Y-%m-%dT%H:%M:%S%z')

def clean_version(version):
    """Clean up package version string by removing suffix after tilde."""
    return version.split('~')[0] if '~' in version else version

def check_reboot_required():
    """Check if system reboot is required."""
    return 'Yes' if os.path.exists(REBOOT_FILE_PATH) else 'No'

def check_system_status():
    """Check for available updates and system reboot status."""
    try:
        upgradable_packages = subprocess.check_output(APT_LIST_CMD, shell=True).decode().strip().split('\n')
        if len(upgradable_packages) > 0 and upgradable_packages[0].startswith('Listing...'):
            upgradable_packages = upgradable_packages[1:]

        security_updates = []
        software_updates = []
        for package in upgradable_packages:
            if not package:
                continue

            package_details = package.split()

            if len(package_details) >= 4 and "upgradable" in package_details[-3]:
                package_name = package_details[0].split('/')[0]
                package_info = {
                    'package_name': package_name,
                    'latest_version': clean_version(package_details[1]),
                    'installed_version': clean_version(package_details[-1].strip('[]')),
                    'security_flag': 'Yes' if 'security' in package_details[0] else 'No'
                }

                if 'security' in package_details[0]:
                    security_updates.append(package_info)
                else:
                    software_updates.append(package_info)
            else:
                logger.warning(f"Invalid package details: {package}")

        reboot_required = check_reboot_required()
        return security_updates, software_updates, reboot_required

    except subprocess.CalledProcessError as e:
        logger.error(f"Error checking for updates: {e}")
        return [], [], False

def get_os_info():
    """Get OS name, version, and codename."""
    os_name, os_version, os_codename = "Unknown", "Unknown", "Unknown"
    try:
        with open("/etc/os-release", "r") as file:
            for line in file.readlines():
                if line.startswith("PRETTY_NAME="):
                    os_name = line.split("=")[1].strip().strip('"').replace("GNU/Linux", "").strip()
                    os_name = " ".join(os_name.split()[:2])
                elif line.startswith("VERSION_CODENAME="):
                    os_codename = line.split("=")[1].strip().strip('"')
    except FileNotFoundError:
        logger.error("OS release file not found")

    try:
        os_version = subprocess.check_output(['uname', '-r']).decode('utf-8').strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Error getting OS version: {e}")

    return os_name, os_version, os_codename

def get_os_date():
    """Get OS build date from uname -v output."""
    try:
        p1 = subprocess.run(['uname', '-v'], stdout=subprocess.PIPE)
        host_werk = p1.stdout.decode('utf-8')
        if '(' in host_werk and ')' in host_werk:
            start = host_werk.index('(')
            end = host_werk.index(')', start + 1)
            return host_werk[start + 1:end]
        else:
            logger.error('OS date format not found in uname -v output.')
            return None
    except Exception as e:
        logger.error(f'Error while trying to obtain OS Date: {str(e)}')
        return None


# ---------------------------------------------------------------------------
# MQTT publish
# ---------------------------------------------------------------------------

def mqtt_publish(topic, payload):
    """Publish a message to the MQTT broker."""
    try:
        result = client.publish(topic, payload, qos=MQTT_QOS, retain=MQTT_RETAIN)
        if not result.is_published():
            result.wait_for_publish()
        logger.info(f'Published to {topic}')
        return True
    except Exception as e:
        logger.error(f"MQTT publish failed for {topic}: {e}")
        return False

def publish_metrics():
    """Publish update metrics to the MQTT broker."""
    try:
        security_updates, software_updates, reboot_needed = check_system_status()
        last_updated = get_last_updated_time()
        last_checked = now.strftime('%Y-%m-%dT%H:%M:%S%z')
        os_name, os_version, os_codename = get_os_info()
        os_date = get_os_date()

        metrics = {
            'reboot_needed': reboot_needed,
            'last_updated': last_updated,
            'last_checked': last_checked,
            'os_name': os_name,
            'os_version': os_version,
            'os_codename': os_codename,
            'os_date': os_date
        }

        # Security updates state
        security_state_topic = f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/security_updates/state'
        mqtt_publish(security_state_topic, json.dumps({"security_updates": len(security_updates)}))
        logger.info(f'Published security_updates state: {len(security_updates)}')

        # Software updates state
        software_state_topic = f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/software_updates/state'
        mqtt_publish(software_state_topic, json.dumps({"software_updates": len(software_updates)}))
        logger.info(f'Published software_updates state: {len(software_updates)}')

        # Other metrics
        for metric, value in metrics.items():
            state_topic = f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/{metric}/state'
            mqtt_publish(state_topic, json.dumps({metric: value}))
            logger.info(f'Published {metric}: {value}')

        # Security update attributes
        security_attributes_topic = f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/security_updates/attributes'
        if security_updates:
            mqtt_publish(security_attributes_topic, json.dumps({"updates": security_updates}))
            logger.info('Published security_updates attributes')
        else:
            mqtt_publish(security_attributes_topic, '')
            logger.info('Cleared security_updates attributes')

        # Software update attributes
        software_attributes_topic = f'{MQTT_TOPIC_PREFIX}/sensor/{hostname}/software_updates/attributes'
        if software_updates:
            mqtt_publish(software_attributes_topic, json.dumps({"updates": software_updates}))
            logger.info('Published software_updates attributes')
        else:
            mqtt_publish(software_attributes_topic, '')
            logger.info('Cleared software_updates attributes')

    except Exception as e:
        logger.error(f"Error during MQTT publishing: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

try:
    logger.info(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    while not connected_flag:
        logger.info("Waiting for MQTT connection...")
        time.sleep(0.1)

except Exception as e:
    logger.error(f"Failed to connect to MQTT broker: {e}")
    exit(1)

try:
    clean_apt_cache()
    update_apt_cache()
    discover_sensors()
    publish_metrics()

except Exception as e:
    logger.error(f"Error during execution: {e}")

finally:
    client.loop_stop()
    client.disconnect()
    logger.info('Disconnected from MQTT broker')
    logger.info('Script completed')