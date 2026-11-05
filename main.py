
import os
import json
import re
import logging
import queue

from datetime import timezone
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ConfigDict, field_validator, ValidationError

from mqtt_client import MQTTClient
from mongodb_manager import MongoDBManager


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

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

if MQTT_USERNAME and not MQTT_PASSWORD:
    raise RuntimeError("MQTT_PASSWORD is required when MQTT_USERNAME is set")


# --------------------------------------------------
# SENSOR DATA VALIDATION
# --------------------------------------------------

class SensorData(BaseModel):

    model_config = ConfigDict(extra="forbid")

    sensor_id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9_-]+$"
    )

    timestamp: object

    readings: dict[str, float]

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, value):
        from datetime import datetime

        if not isinstance(value, str):
            raise ValueError("Timestamp must be an ISO 8601 string")

        try:
            parsed = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        except ValueError:
            raise ValueError("Invalid timestamp format")

        if parsed.tzinfo is None:
            raise ValueError("Timestamp must include timezone")

        return parsed.astimezone(timezone.utc)

    @field_validator("readings", mode="before")
    @classmethod
    def validate_readings(cls, value):

        import math

        if not isinstance(value, dict) or not value:
            raise ValueError("Readings must be a non-empty object")

        for name, reading in value.items():

            if not isinstance(name, str):
                raise ValueError("Measurement names must be strings")

            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name):
                raise ValueError(f"Invalid measurement name: {name}")

            if isinstance(reading, bool) or not isinstance(
                reading, (int, float)
            ):
                raise ValueError(f"{name} must be numeric")

            if not math.isfinite(reading):
                raise ValueError(f"{name} must be finite")

        return value


# --------------------------------------------------
# SENSOR MEASUREMENT FILTERS
# --------------------------------------------------

MEASUREMENT_RANGES = {
    "temperature": (-40, 85),
    "humidity": (0, 100),
    "pressure": (300, 1200)
}


def filter_sensor_data(data):

    for measurement, value in data.readings.items():

        if measurement not in MEASUREMENT_RANGES:
            raise ValueError(
                f"Unsupported measurement: {measurement}"
            )

        minimum, maximum = MEASUREMENT_RANGES[measurement]

        if not minimum <= value <= maximum:
            raise ValueError(
                f"{measurement} out of range: {value}"
            )

    return True


# --------------------------------------------------
# MQTT MESSAGE PROCESSING
# --------------------------------------------------

TOPIC_PATTERN = re.compile(
    r"^sensors/(?P<sensor_id>[A-Za-z0-9_-]{1,64})/data$"
)

MAX_PAYLOAD_SIZE = 64 * 1024


def reject_json_constant(value):
    raise ValueError(f"Invalid JSON numeric constant: {value}")


def process_message(topic, payload, database):

    # 1. Validate MQTT topic

    match = TOPIC_PATTERN.fullmatch(topic)

    if not match:
        logger.warning("Rejected unexpected topic: %s", topic)
        return

    topic_sensor_id = match.group("sensor_id")

    try:

        # 2. Check payload size

        if len(payload) > MAX_PAYLOAD_SIZE:
            raise ValueError("Payload exceeds maximum allowed size")

        # 3. Decode bytes

        raw_message = payload.decode("utf-8")

        # 4. Parse JSON

        parsed_message = json.loads(
            raw_message,
            parse_constant=reject_json_constant
        )

        if not isinstance(parsed_message, dict):
            raise ValueError("JSON payload must be an object")

        # 5. Validate structure and data types

        sensor_data = SensorData.model_validate(parsed_message)

        # 6. Check topic sensor ID against JSON sensor ID

        if sensor_data.sensor_id != topic_sensor_id:
            raise ValueError(
                "Sensor ID mismatch between MQTT topic and payload"
            )

        # 7. Apply measurement filters

        filter_sensor_data(sensor_data)

        # 8. Prepare MongoDB document

        document = sensor_data.model_dump()

        # 9. Store in sensor-specific collection

        database.insert_sensor_data(
            sensor_id=sensor_data.sensor_id,
            document=document
        )

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValidationError,
        ValueError
    ) as error:

        logger.warning(
            "Rejected message from %s: %s",
            topic,
            error
        )


# --------------------------------------------------
# MAIN APPLICATION
# --------------------------------------------------

def main():

    message_queue = queue.Queue(maxsize=10000)

    database = None
    mqtt_client = None

    try:

        # Initialize MongoDB

        database = MongoDBManager(
            connection_string=MONGO_URI,
            database_name=MONGO_DATABASE
        )

        # Initialize MQTT

        mqtt_client = MQTTClient(
            broker=MQTT_BROKER,
            port=MQTT_PORT,
            username=MQTT_USERNAME,
            password=MQTT_PASSWORD,
            client_id=MQTT_CLIENT_ID,
            message_queue=message_queue,
            use_tls=MQTT_TLS
        )

        # Start MQTT connection

        mqtt_client.connect()

        logger.info("IoT data processing service started")

        # Main processing loop

        while True:

            try:
                topic, payload = message_queue.get(timeout=1)

                process_message(
                    topic=topic,
                    payload=payload,
                    database=database
                )

            except queue.Empty:
                continue

            except Exception:
                logger.exception("Unexpected message processing error")

    except KeyboardInterrupt:

        logger.info("Shutdown requested")

    except Exception:

        logger.exception("Application startup or runtime failure")

        raise

    finally:

        if mqtt_client:
            mqtt_client.disconnect()

        if database:
            database.close()

        logger.info("Application stopped")


if __name__ == "__main__":
    main()
