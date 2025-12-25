import appdaemon.plugins.hass.hassapi as hass
import time
import json
import os
from datetime import datetime
from threading import Lock


class RFBridgeDemux(hass.Hass):

    def initialize(self):
        # ==========================================================
        # CONFIG
        # ==========================================================
        self.debounce_seconds = float(self.args.get("debounce_seconds", 1.5))
        self.unknown_base_topic = self.args.get("unknown_base_topic", "home/unknown")
        self.discovery_prefix = self.args.get("discovery_prefix", "homeassistant")
        self.default_off_delay = int(self.args.get("default_off_delay", 60))

        self.notify_service = self.args.get("notify_service")

        # --- Logging (MATCH NotificationLogger) ---
        self.log_dir = self.args.get("log_dir", "/config/www")
        os.makedirs(self.log_dir, exist_ok=True)
        self.file_lock = Lock()

        # --- Persistence files ---
        self.persist_unknowns_file = self.args.get(
            "persist_unknowns_file", "/config/rfbridge_discovered_unknowns.json"
        )
        self.counters_file = self.args.get(
            "counters_file", "/config/rfbridge_counters.json"
        )
        self.signal_stats_file = self.args.get(
            "signal_stats_file", "/config/rfbridge_signal_stats.json"
        )

        # ==========================================================
        # STATE
        # ==========================================================
        self.last_seen = {}
        self.discovered_unknowns = set()
        self.counters = {}
        self.signal_stats = {}

        # ==========================================================
        # RF MAP — ALL YOUR ORIGINAL SENSORS
        # ==========================================================
        self.RF_MAP = {
            "FF1101": {"topic": "EXAMPLE 1", "payload": "ON"},
            "B41101": {"topic": "EXAMPLE 2", "payload": "ON"}
        }

        # ==========================================================
        # LOAD PERSISTED DATA
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

        self.listen_event(self.handle_mqtt, "mqtt_message", namespace="mqtt")

    # ==========================================================
    # MQTT HANDLER (TASMOTA RF BRIDGE)
    # ==========================================================
    def handle_mqtt(self, event_name, data, kwargs):
        topic = data.get("topic", "")
        payload = data.get("payload")

        if not topic.endswith("/RESULT") or payload is None:
            return

        try:
            msg = json.loads(payload)
        except Exception:
            return

        rf = msg.get("RfReceived")
        if not rf:
            return

        code = rf.get("Data")
        if not code:
            return

        code = code.strip().upper()
        bridge = topic.split("/")[1] if "/" in topic else "unknown_bridge"

        low = rf.get("Low")
        high = rf.get("High")
        sync = rf.get("Sync")

        now = time.time()
        last = self.last_seen.get(code)
        if last and (now - last) < self.debounce_seconds:
            return
        self.last_seen[code] = now

        self._log_activity(code, bridge)

        if low and high and sync:
            self._update_signal_stats(code, bridge, low, high, sync)

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

        self._handle_unknown(code)

    # ==========================================================
    # UNKNOWN RF HANDLING (MQTT DISCOVERY)
    # ==========================================================
    def _handle_unknown(self, code):
        state_topic = f"{self.unknown_base_topic}/{code}"
        object_id = f"rf_{code.lower()}"
        discovery_topic = f"{self.discovery_prefix}/binary_sensor/{object_id}/config"

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
            self._notify(f"🆕 New RF sensor discovered: {code}")

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
        ts = datetime.now().isoformat(timespec="seconds")

        entry = self.signal_stats.setdefault(code, {"bridges": {}, "best_bridge": None})
        b = entry["bridges"].setdefault(
            bridge,
            {"count": 0, "avg_low": 0.0, "avg_high": 0.0, "avg_sync": 0.0, "last_seen": None},
        )

        b["count"] += 1
        b["avg_low"] += (low - b["avg_low"]) / b["count"]
        b["avg_high"] += (high - b["avg_high"]) / b["count"]
        b["avg_sync"] += (sync - b["avg_sync"]) / b["count"]
        b["last_seen"] = ts

        best = max(
            entry["bridges"].items(),
            key=lambda x: (x[1]["count"], -x[1]["avg_sync"]),
        )[0]

        entry["best_bridge"] = best
        self.signal_stats[code] = entry
        self._save_json_dict(self.signal_stats_file, self.signal_stats)

        self._check_signal_degradation(code, b)
        self._check_battery_health(code, b)

    # ==========================================================
    # ALERTS
    # ==========================================================
    def _check_signal_degradation(self, code, b):
        if b["count"] >= 10 and b["avg_sync"] > 26000:
            self._notify(f"⚠ RF {code}: signal degradation")

    def _check_battery_health(self, code, b):
        if b["count"] >= 20 and (b["avg_high"] > 2600 or b["avg_low"] > 900):
            self._notify(f"🔋 RF {code}: possible low battery")

    # ==========================================================
    # ACTIVITY LOGGING (MATCH NotificationLogger)
    # ==========================================================
    def _log_activity(self, code, bridge):
        now = datetime.now()
        date_str = now.date().isoformat()

        log_file = f"{self.log_dir}/rfbridge_activity_{date_str}.jsonl"

        entry = {
            "timestamp": now.strftime("%d-%m %H:%M:%S"),
            "code": code,
            "bridge": bridge,
            "known": code in self.RF_MAP,
        }

        line = json.dumps(entry, ensure_ascii=False) + "\n"

        try:
            with self.file_lock:
                with open(log_file, "a", encoding="utf-8") as f:
                    f.write(line)
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
        try:
            if not os.path.exists(path):
                return set()
            with open(path, "r") as f:
                return set(json.load(f))
        except Exception:
            return set()

    def _save_json_set(self, path, s):
        self._atomic_write(path, list(s))

    def _load_json_dict(self, path):
        try:
            if not os.path.exists(path):
                return {}
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_json_dict(self, path, d):
        self._atomic_write(path, d)

    def _atomic_write(self, path, obj):
        tmp = f"{path}.tmp"
        with open(tmp, "w") as f:
            json.dump(obj, f, indent=2)
        os.replace(tmp, path)

