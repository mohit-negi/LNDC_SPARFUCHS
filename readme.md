# LNDC Event, Here we go

Wilkommen Zwickau


### Python (`requirements.txt` or `pyproject.toml`)

Install Python and create a virtual environment:

```sh
python -m venv .venv
```

Activate it in PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

On macOS/Linux:

```sh
source .venv/bin/activate
```

Go through the `requirements.txt`, install its packages:

```sh
python -m pip install -r requirements.txt
```

```

```

### Configure the backend

The backend needs a reachable MQTT broker and MongoDB instance. Set the required
environment variables before starting it. You can put these values in a `.env`
file in the project directory; `main.py` loads it automatically.

```env
MQTT_BROKER=localhost
MONGO_URI=mongodb://localhost:27017

# Optional settings
MQTT_PORT=1883
MQTT_USERNAME=
MQTT_PASSWORD=
MQTT_CLIENT_ID=iot_backend
MQTT_TLS=false
MONGO_DATABASE=sensor_data
API_HOST=127.0.0.1
API_PORT=8000
```

Do not commit real passwords or other secrets. `MQTT_BROKER` and `MONGO_URI`
must be set. The optional settings above use the shown defaults when omitted.

### Run the backend

With the virtual environment activated and dependencies installed, start the
API and MQTT consumer with:

```sh
python main.py
```

The API listens at `http://127.0.0.1:8000` by default. Change `API_HOST` or
`API_PORT` to use a different address.

### Send sensor data

Publish JSON messages to the MQTT topic `sensors/<sensor_id>/data`. The
`sensor_id` in the topic must match the one in the JSON payload. Timestamps
must include a timezone. Supported readings are `temperature` (-40 to 85),
`humidity` (0 to 100), and `pressure` (300 to 1200).

```json
{
  "sensor_id": "sensor-1",
  "timestamp": "2026-11-05T12:00:00Z",
  "readings": {
    "temperature": 22.5,
    "humidity": 48
  }
}
```

### REST API

- `GET /api/v1/health` — check API and database availability.
- `GET /api/v1/sensors` — list sensor collections.
- `GET /api/v1/sensors/{sensor_id}` — get a sensor and its latest reading.
- `GET /api/v1/sensors/{sensor_id}/latest` — get its latest reading.
- `GET /api/v1/sensors/{sensor_id}/readings` — retrieve readings, with optional
  `start`, `end`, `limit` (1–1000), and `skip` query parameters.

FastAPI's interactive API documentation is available at
`http://127.0.0.1:8000/docs` while the service is running.
