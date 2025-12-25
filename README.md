# RFBridge Demux (AppDaemon)

RFBridge Demux is an AppDaemon app for Home Assistant that converts raw RF data
from one or more Tasmota RF Bridges into clean, reliable MQTT topics and entities.

It supports known RF sensors, automatic discovery of unknown sensors, activity
logging, and signal-quality tracking — without cluttering Home Assistant YAML.

---

## Features

- Listens to `tele/<tasmota>/RESULT` MQTT messages
- Supports multiple RF bridges simultaneously
- Maps known RF codes to stable MQTT topics (`home/<sensor>`)
- Automatically detects and quarantines unknown RF sensors
- Creates Home Assistant entities via MQTT Discovery
- Logs RF activity to daily JSONL files
- Tracks RF signal timing values (Low / High / Sync)
- Persists discovered sensors and statistics across restarts

---

## How it works

1. A Tasmota RF Bridge publishes an RF event to MQTT
2. AppDaemon receives the MQTT event
3. The RF code is extracted from `RfReceived.Data`
4. The code is matched against `RF_MAP`
   - **Known** → published to `home/<sensor>`
   - **Unknown** → published to `home/unknown/<code>` and auto-discovered
5. Activity and signal data are written to disk

---

## File locations

| Purpose | Path |
|------|------|
| App code | `/homeassistant/appdaemon/apps/rfbridge_demux.py` |
| Daily activity logs | `/homeassistant/www/rfbridge_activity_YYYY-MM-DD.jsonl` |
| Signal statistics | `/homeassistant/www/rfbridge_signal_stats.json` |
| Event counters | `/homeassistant/www/rfbridge_counters.json` |
| Persisted unknown sensors | `/homeassistant/rfbridge_discovered_unknowns.json` |

---

## AppDaemon configuration

### apps.yaml (minimal)

```yaml
rfbridge_demux:
  module: rfbridge_demux
  class: RFBridgeDemux
