# BerkeleyAlarms — Centralized Alarm Service

Centralized alarm lifecycle manager for the Berkeley Home Intelligence Platform.

Agents publish to `home/alerts/*` as before. `BerkeleyAlarms` subscribes,
owns alarm state, drives Alexa repeat announcements on your schedule, and
exposes a real-time dashboard UI.

## Architecture

```
home/alerts/earthquake  ──┐
home/alerts/leak/#      ──┤  SUBSCRIBE   ┌──────────────────────┐
home/events/earthquake  ──┤ ──────────► │   AlarmManager        │
                           │             │  (state machine)      │
                           │             │  ┌──────────────────┐ │
                           │             │  │ ACTIVE → ACKED   │ │
                           │             │  │   → RESOLVED     │ │
                           │             │  └──────────────────┘ │
                           │             └────────────┬──────────┘
                           │                          │ PUBLISH
                           │                          ▼
                           │      home/commands/alexa-say  (Alexa TTS)
                           │      home/commands/display    (TV banner)
                           │      home/alarms/active       (state broadcast)
                           │
                           │      FastAPI (port 8084)
                           └──── GET /  → Alarm Dashboard UI
                                 GET /alarms
                                 GET /alarms/history
                                 POST /alarms/{id}/ack
                                 POST /alarms/{id}/resolve
                                 WS  /ws/alarms  (live updates)
```

## Alarm Behavior

| Alarm | Trigger | TTS | Repeat | Auto-resolve |
|-------|---------|-----|--------|-------------|
| Earthquake | `home/alerts/earthquake` | "Alexa Announce Earthquake Imminent" | Every 8 s | When `home/events/earthquake` fires, or after 5 min timeout |
| Leak | `home/alerts/leak/#` | "Alexa Announce Leak Detected in {location}" | Every 3 min | When sensor clears AND user has Acked |

## Alarm States

- **ACTIVE** — Alexa announcing on repeat schedule
- **ACKED** — User pressed Acknowledge; Alexa goes silent; badge stays in UI
- **RESOLVED** — Closed; archived to SQLite history

## Quick Start

```bash
# 1. Clone
git clone https://github.com/gavinfischer-keenan/BerkeleyAlarms.git
cd BerkeleyAlarms

# 2. Configure
cp .env.example .env
nano .env  # set MQTT_BROKER

# 3. Run (local dev)
pip install -e .
alarm-service

# 4. Run (Docker — part of BerkeleyPlatform compose)
docker compose up -d alarm-service
```

## Dashboard

Open `http://NODE01_IP:8084` in a browser or on the 4K TV kiosk.

## Adding New Alarms

Edit `alarms.yml` — no code changes required:

```yaml
alarms:
  my-new-alarm:
    name: "My New Alarm"
    trigger_topic: "home/alerts/my-agent/#"
    severity: "warning"
    tts_template: "Alexa Announce {location} sensor triggered"
    repeat_interval_sec: 120
    location_field: "data.location"
    auto_resolve:
      timeout_sec: 600
```

Restart the service and it picks up the new definition.

## MQTT Topics

| Topic | Direction | Description |
|-------|-----------|-------------|
| `home/alerts/#` | Subscribe | All alert triggers from agents |
| `home/events/#` | Subscribe | Auto-resolve signals |
| `home/commands/alexa-say` | Publish | Alexa TTS announcements |
| `home/commands/display` | Publish | Dashboard banner commands |
| `home/alarms/active` | Publish (retained) | Current alarm state for any subscriber |
| `home/status/alarm-service` | Publish (retained) | Service heartbeat |
