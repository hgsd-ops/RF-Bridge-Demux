import appdaemon.plugins.hass.hassapi as hass
import time
import json
import os
from datetime import datetime
from threading import Lock


class RFBridgeDemux(hass.Hass):
    """
    RFBridgeDemux
    =============

    AppDaemon app that listens for MQTT messages from a Tasmota RF Bridge.
    Edit the RF_MAP to match your sensors code. 

    High-level behavior:
    - Listens for MQTT RESULT messages from RF bridges
    - Matches RF codes against a known RF_MAP
    - Publishes known codes to predefined MQTT topics
    - Automatically discovers unknown RF codes as Home Assistant binary sensors
    - Tracks signal quality per RF code and per RF bridge
    - Logs RF activity and statistics to JSON/JSONL files
    - Optionally sends notifications for new sensors or signal issues
    """

    def initialize(self):
        # ==========================================================
        # CONFIGURATION (overridable via apps.yaml)
        # ==========================================================

        # Minimum time (seconds) between identical RF code events
        # Prevents RF spam and repeated triggers
        self.debounce_seconds = float(self.args.get("debounce_seconds", 1.5))

        # Base MQTT topic used for unknown RF sensors
        self.unknown_base_topic = self.args.get("unknown_base_topic", "home/unknown")

        # Home Assistant MQTT discovery prefix
        self.discovery_prefix = self.args.get("discovery_prefix", "homeassistant")

        # Auto-off delay (seconds) for discovered binary sensors
        self.default_off_delay = int(self.args.get("default_off_delay", 60))

        # Optional notify service (example: notify/mobile_app_phone)
        self.notify_service = self.args.get("notify_service")

        # ==========================================================
        # LOGGING AND STORAGE
        # ==========================================================

        # Directory used for logs and persistent JSON files
        self.log_dir = self.args.get("log_dir", "/homeassistant/www/logs")

        # File lock to avoid concurrent write corruption
        self.file_lock = Lock()

        # Startup safety checks for log directory
        if not os.path.isdir(self.log_dir):
            self.log(
                f"log_dir does not exist: {self.log_dir}",
                level="WARNING",
            )
        elif not os.access(self.log_dir, os.W_OK):
            self.log(
                f"log_dir is not writable: {self.log_dir}",
                level="WARNING",
            )
        else:
            os.makedirs(self.log_dir, exist_ok=True)

        # ----------------------------------------------------------
        # Persistent storage files
        # ----------------------------------------------------------

        # Stores RF codes that have already been auto-discovered
        self.persist_unknowns_file = self.args.get(
            "persist_unknowns_file",
            f"{self.log_dir}/rfbridge_discovered_unknowns.json"
        )

        # Stores counters and last-seen information per RF code
        self.counters_file = self.args.get(
            "counters_file",
            f"{self.log_dir}/rfbridge_counters.json"
        )

        # Stores signal statistics (timings, bridge quality, etc.)
        self.signal_stats_file = self.args.get(
            "signal_stats_file",
            f"{self.log_dir}/rfbridge_signal_stats.json"
        )

        # ==========================================================
        # RUNTIME STATE (in-memory)
        # ==========================================================

        # Last time each RF code was seen (for debouncing)
        self.last_seen = {}

        # Set of RF codes already discovered via MQTT discovery
        self.discovered_unknowns = set()

        # Per-RF code counters and metadata
        self.counters = {}

        # Signal quality statistics per RF code and bridge
        self.signal_stats = {}

        # ==========================================================
        # RF MAP (USER-DEFINED KNOWN RF CODES)
        # ==========================================================
        # Map RF codes to MQTT topics and payloads.
        # When a matching RF code is received, the corresponding
        # MQTT message is published.
        self.RF_MAP = {
            "FF1101": {"topic": "EXAMPLE 1", "payload": "ON"},
            "B41101": {"topic": "EXAMPLE 2", "payload": "ON"}
        }

        # ==========================================================
        # LOAD PERSISTED DATA FROM DISK
        # ==========================================================
        self.discovered_unknowns = self._load_json_set(self.persist_unknowns_file)
        self.counters = self._load_json_dict(self.counters_file)
        self.signal_stats = self._load_json_dict(self.signal_stats_file)

        self.log(
            f"RFBridgeDemux started | "
            f"{len(self.RF_MAP)} known RF codes | "
            f"{len(self.discovered_unknowns)} unknowns",
            level="INFO"
        )

        # Subscribe to MQTT events from AppDaemon MQTT plugin
        self.listen_event(self.handle_mqtt, "mqtt_message", namespace="mqtt")

    # ==========================================================
    # MQTT HANDLER (TASMOTA RF BRIDGE)
    # ==========================================================
    def handle_mqtt(self, event_name, data, kwargs):
        """
        Handles incoming MQTT messages from the RF bridge.

        Expected topic format:
          tele/<bridge_name>/RESULT

        Expected payload format:
          { "RfReceived": { "Data": "...", "Low": ..., "High": ..., "Sync": ... } }
        """

        topic = data.get("topic", "")
        payload = data.get("payload")

        # Only process RF RESULT messages
        if not topic.endswith("/RESULT") or payload is None:
            return

        # Parse JSON payload
        try:
            msg = json.loads(payload)
        except Exception:
            return

        rf = msg.get("RfReceived")
        if not rf:
            return

        # Extract RF code
        code = rf.get("Data")
        if not code:
            return

        code = code.strip().upper()

        # Extract bridge name from topic
        bridge = topic.split("/")[1] if "/" in topic else "unknown_bridge"

        # RF timing values (used for signal quality)
        low = rf.get("Low")
        high = rf.get("High")
        sync = rf.get("Sync")

        # Debounce identical RF codes
        now = time.time()
        last = self.last_seen.get(code)
        if last and (now - last) < self.debounce_seconds:
            return
        self.last_seen[code] = now

        # Log activity and update counters
        self._log_activity(code, bridge)

        # Update signal statistics if timing data is present
        if low and high and sync:
            self._update_signal_stats(code, bridge, low, high, sync)

        # Known RF code → publish predefined MQTT message
        if code in self.RF_MAP:
            entry = self.RF_MAP[code]
            self.call_service(
                "mqtt/publish",
                namespace="mqtt",
                topic=f"home/{entry['topic']}",
                payload=entry["payload"],
                qos=0,
                retain=False,
            )
            return

        # Unknown RF code → auto-discovery
        self._handle_unknown(code)

    # ==========================================================
    # UNKNOWN RF HANDLING (MQTT DISCOVERY)
    # ==========================================================
    def _handle_unknown(self, code):
        """
        Creates a Home Assistant MQTT-discovered binary sensor
        for previously unseen RF codes.
        """

        state_topic = f"{self.unknown_base_topic}/{code}"
        object_id = f"rf_{code.lower()}"
        discovery_topic = f"{self.discovery_prefix}/binary_sensor/{object_id}/config"

        # Only publish discovery once per RF code
        if code not in self.discovered_unknowns:
            discovery_payload = {
                "name": f"RF {code}",
                "unique_id": f"rf_unknown_{code.lower()}",
                "state_topic": state_topic,
                "payload_on": "ON",
                "off_delay": self.default_off_delay,
                "device_class": "opening",
            }

            self.call_service(
                "mqtt/publish",
                namespace="mqtt",
                topic=discovery_topic,
                payload=json.dumps(discovery_payload),
                retain=True,
            )

            self.discovered_unknowns.add(code)
            self._save_json_set(self.persist_unknowns_file, self.discovered_unknowns)
            self._notify(f"New RF sensor discovered: {code}")

        # Publish sensor state
        self.call_service(
            "mqtt/publish",
            namespace="mqtt",
            topic=state_topic,
            payload="ON",
            qos=0,
            retain=False,
        )

    # ==========================================================
    # SIGNAL QUALITY + BEST BRIDGE
    # ==========================================================
    def _update_signal_stats(self, code, bridge, low, high, sync):
        """
        Tracks RF signal timing statistics per bridge and
        determines which bridge receives the signal best.
        """

        ts = datetime.now().isoformat(timespec="seconds")

        entry = self.signal_stats.setdefault(code, {"bridges": {}, "best_bridge": None})
        b = entry["bridges"].setdefault(
            bridge,
            {"count": 0, "avg_low": 0.0, "avg_high": 0.0, "avg_sync": 0.0, "last_seen": None},
        )

        # Incremental averages
        b["count"] += 1
        b["avg_low"] += (low - b["avg_low"]) / b["count"]
        b["avg_high"] += (high - b["avg_high"]) / b["count"]
        b["avg_sync"] += (sync - b["avg_sync"]) / b["count"]
        b["last_seen"] = ts

        # Select best bridge by count, then by sync quality
        best = max(
            entry["bridges"].items(),
            key=lambda x: (x[1]["count"], -x[1]["avg_sync"]),
        )[0]

        entry["best_bridge"] = best
        self.signal_stats[code] = entry
        self._save_json_dict(self.signal_stats_file, self.signal_stats)

        # Check for potential issues
        self._check_signal_degradation(code, b)
        self._check_battery_health(code, b)

    # ==========================================================
    # ALERTS
    # ==========================================================
    def _check_signal_degradation(self, code, b):
        """Warn if RF timing indicates degraded signal quality."""
        if b["count"] >= 10 and b["avg_sync"] > 26000:
            self._notify(f"RF {code}: signal degradation")

    def _check_battery_health(self, code, b):
        """Warn if RF timing suggests low transmitter battery."""
        if b["count"] >= 20 and (b["avg_high"] > 2600 or b["avg_low"] > 900):
            self._notify(f"RF {code}: possible low battery")

    # ==========================================================
    # ACTIVITY LOGGING
    # ==========================================================
    def _log_activity(self, code, bridge):
        """
        Appends RF activity events to a daily JSONL file and
        updates per-code counters.
        """

        now = datetime.now()
        date_str = now.date().isoformat()
        log_file = f"{self.log_dir}/rfbridge_activity_{date_str}.jsonl"

        entry = {
            "timestamp": now.strftime("%d-%m %H:%M:%S"),
            "code": code,
            "bridge": bridge,
            "known": code in self.RF_MAP,
        }

        try:
            with self.file_lock:
                with open(log_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry) + "\n")
        except Exception as e:
            self.log(f"RF WRITE FAILED: {type(e).__name__}: {e}", level="ERROR")

        c = self.counters.get(code, {"count": 0})
        c["count"] += 1
        c["last_seen"] = now.isoformat(timespec="seconds")
        c["last_bridge"] = bridge
        c["known"] = code in self.RF_MAP
        self.counters[code] = c
        self._save_json_dict(self.counters_file, self.counters)

    # ==========================================================
    # NOTIFY
    # ==========================================================
    def _notify(self, message):
        """Send a Home Assistant notification if configured."""
        if not self.notify_service:
            return
        try:
            domain, service = self.notify_service.split("/", 1)
            self.call_service(f"{domain}/{service}", message=message)
        except Exception as e:
            self.log(f"Notify failed: {e}", level="WARNING")

    # ==========================================================
    # FILE HELPERS
    # ==========================================================
    def _load_json_set(self, path):
        """Load a JSON list from disk and return it as a set."""
        try:
            if not os.path.exists(path):
                return set()
            with open(path, "r") as f:
                return set(json.load(f))
        except Exception:
            return set()

    def _save_json_set(self, path, s):
        """Atomically write a set to disk as a JSON list."""
        self._atomic_write(path, list(s))

    def _load_json_dict(self, path):
        """Load a JSON dict from disk."""
        try:
            if not os.path.exists(path):
                return {}
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_json_dict(self, path, d):
        """Atomically write a JSON dict to disk."""
        self._atomic_write(path, d)

    def _atomic_write(self, path, obj):
        """Write JSON to disk atomically to avoid corruption."""
        tmp = f"{path}.tmp"
        with open(tmp, "w") as f:
            json.dump(obj, f, indent=2)
        os.replace(tmp, path)

