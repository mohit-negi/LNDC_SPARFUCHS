
import os
import re
import json
import queue
import logging
import asyncio

from datetime import datetime, timezone
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field, ConfigDict, field_validator, ValidationError
import uvicorn

from mqtt_client import MQTTClient
from mongodb_manager import MongoDBManager


# ==================================================
# LOGGING
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ==================================================
# CONFIGURATION
# ==================================================

load_dotenv()


def get_required_env(name):
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")

    return value


MQTT_BROKER = get_required_env("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "iot_backend")
MQTT_TLS = os.getenv("MQTT_TLS", "false").lower() == "true"

MONGO_URI = get_required_env("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "sensor_data")

API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "8000"))


# ==================================================
# SENSOR DATA MODEL
# ==================================================

class SensorData(BaseModel):

    model_config = ConfigDict(extra="forbid")

    sensor_id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9_-]+$"
    )

    timestamp: datetime

    readings: dict[str, float]

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value):

        if value.tzinfo is None:
            raise ValueError("Timestamp must include timezone")

        return value.astimezone(timezone.utc)

    @field_validator("readings", mode="before")
    @classmethod
    def validate_readings(cls, value):

        import math

        if not isinstance(value, dict) or not value:
            raise ValueError("Readings must be a non-empty object")

        for name, reading in value.items():

            if not isinstance(name, str):
                raise ValueError("Invalid measurement name")

            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name):
                raise ValueError("Invalid measurement name")

            if isinstance(reading, bool) or not isinstance(
                reading, (int, float)
            ):
                raise ValueError(f"{name} must be numeric")

            if not math.isfinite(reading):
                raise ValueError(f"{name} must be finite")

        return value


# ==================================================
# SENSOR VALIDATION
# ==================================================

MEASUREMENT_RANGES = {
    "temperature": (-40, 85),
    "humidity": (0, 100),
    "pressure": (300, 1200)
}


def filter_sensor_data(data):

    for name, value in data.readings.items():

        if name not in MEASUREMENT_RANGES:
            raise ValueError(f"Unsupported measurement: {name}")

        minimum, maximum = MEASUREMENT_RANGES[name]

        if not minimum <= value <= maximum:
            raise ValueError(f"{name} outside permitted range")

    return True


# ==================================================
# MQTT MESSAGE PROCESSING
# ==================================================

TOPIC_PATTERN = re.compile(
    r"^sensors/(?P<sensor_id>[A-Za-z0-9_-]{1,64})/data$"
)

MAX_PAYLOAD_SIZE = 64 * 1024


def reject_json_constant(value):
    raise ValueError(f"Invalid JSON constant: {value}")


def process_message(topic, payload, database):

    match = TOPIC_PATTERN.fullmatch(topic)

    if not match:
        logger.warning("Invalid MQTT topic: %s", topic)
        return

    topic_sensor_id = match.group("sensor_id")

    try:

        if len(payload) > MAX_PAYLOAD_SIZE:
            raise ValueError("Payload too large")

        raw_message = payload.decode("utf-8")

        parsed = json.loads(
            raw_message,
            parse_constant=reject_json_constant
        )

        sensor_data = SensorData.model_validate(parsed)

        if sensor_data.sensor_id != topic_sensor_id:
            raise ValueError("Sensor ID mismatch")

        filter_sensor_data(sensor_data)

        database.insert_sensor_data(
            sensor_data.sensor_id,
            sensor_data.model_dump()
        )

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValidationError,
        ValueError
    ) as error:

        logger.warning("Rejected MQTT message: %s", error)


# ==================================================
# REST API
# ==================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    logger.info("REST API started")

    yield

    logger.info("REST API stopped")


app = FastAPI(
    title="IoT Sensor API",
    description="REST API for retrieving validated sensor data",
    version="1.0.0",
    lifespan=lifespan
)


def get_database(request: Request):
    database = getattr(request.app.state, "database", None)

    if database is None:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable"
        )

    return database


@app.get("/api/v1/health")
def health_check(request: Request):

    database = get_database(request)

    try:
        database.client.admin.command("ping")

        return {
            "status": "healthy",
            "database": "connected"
        }

    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable"
        )


@app.get("/api/v1/sensors")
def get_sensors(request: Request):

    database = get_database(request)

    return {
        "sensors": database.list_sensors()
    }


@app.get("/api/v1/sensors/{sensor_id}")
def get_sensor(sensor_id: str, request: Request):

    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", sensor_id):
        raise HTTPException(400, "Invalid sensor ID")

    database = get_database(request)

    if not database.sensor_exists(sensor_id):
        raise HTTPException(404, "Sensor not found")

    latest = database.get_latest_reading(sensor_id)

    return {
        "sensor_id": sensor_id,
        "latest_reading": latest
    }


@app.get("/api/v1/sensors/{sensor_id}/latest")
def get_latest(sensor_id: str, request: Request):

    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", sensor_id):
        raise HTTPException(400, "Invalid sensor ID")

    database = get_database(request)

    result = database.get_latest_reading(sensor_id)

    if result is None:
        raise HTTPException(404, "Sensor not found")

    return result


@app.get("/api/v1/sensors/{sensor_id}/readings")
def get_readings(
    sensor_id: str,
    request: Request,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    skip: int = Query(default=0, ge=0)
):

    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", sensor_id):
        raise HTTPException(400, "Invalid sensor ID")

    # Validate timezone-aware date parameters

    for date_value in (start, end):

        if date_value and date_value.tzinfo is None:
            raise HTTPException(
                400,
                "Date parameters must include timezone"
            )

    if start and end and start > end:
        raise HTTPException(
            400,
            "Start date must be before end date"
        )

    if start:
        start = start.astimezone(timezone.utc)

    if end:
        end = end.astimezone(timezone.utc)

    database = get_database(request)

    results = database.get_sensor_readings(
        sensor_id=sensor_id,
        start=start,
        end=end,
        limit=limit,
        skip=skip
    )

    if results is None:
        raise HTTPException(404, "Sensor not found")

    return jsonable_encoder({
        "sensor_id": sensor_id,
        "count": len(results),
        "limit": limit,
        "skip": skip,
        "readings": results
    })


# ==================================================
# MQTT CONSUMER
# ==================================================

async def consume_messages(message_queue, database, stop_event):

    while not stop_event.is_set():

        try:

            topic, payload = await asyncio.to_thread(
                message_queue.get,
                True,
                1
            )

            try:
                await asyncio.to_thread(
                    process_message,
                    topic,
                    payload,
                    database
                )
            finally:
                message_queue.task_done()

        except queue.Empty:
            continue


# ==================================================
# MAIN APPLICATION
# ==================================================

async def main():

    message_queue = queue.Queue(maxsize=10000)

    database = None
    mqtt_client = None
    stop_event = asyncio.Event()

    try:

        # Connect to MongoDB

        database = MongoDBManager(
            MONGO_URI,
            MONGO_DATABASE
        )

        # Make database available to REST API

        app.state.database = database

        # Start MQTT

        mqtt_client = MQTTClient(
            broker=MQTT_BROKER,
            port=MQTT_PORT,
            username=MQTT_USERNAME,
            password=MQTT_PASSWORD,
            client_id=MQTT_CLIENT_ID,
            message_queue=message_queue,
            use_tls=MQTT_TLS
        )

        mqtt_client.connect()

        # Start REST API

        config = uvicorn.Config(
            app,
            host=API_HOST,
            port=API_PORT,
            log_level="info"
        )

        server = uvicorn.Server(config)

        logger.info(
            "Starting REST API at http://%s:%s",
            API_HOST,
            API_PORT
        )

        server_task = asyncio.create_task(server.serve())

        consumer_task = asyncio.create_task(
            consume_messages(
                message_queue,
                database,
                stop_event
            )
        )

        # Wait for API server to stop

        await server_task

    except KeyboardInterrupt:

        logger.info("Shutdown requested")

    finally:

        stop_event.set()

        if mqtt_client:
            mqtt_client.disconnect()

        if database:
            database.close()

        logger.info("Backend stopped")


if __name__ == "__main__":
    asyncio.run(main())
