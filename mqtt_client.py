
import logging
import queue

import paho.mqtt.client as mqtt


logger = logging.getLogger(__name__)


class MQTTClient:

    def __init__(
        self,
        broker,
        port,
        username,
        password,
        client_id,
        message_queue,
        use_tls=False
    ):
        self.message_queue = message_queue

        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv311
        )

        if username:
            self.client.username_pw_set(username, password)

        if use_tls:
            self.client.tls_set()

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        self.broker = broker
        self.port = port

    def on_connect(
        self, client, userdata, flags, reason_code, properties
    ):
        if reason_code == 0:
            logger.info("Connected to MQTT broker")

            client.subscribe("sensors/+/data", qos=1)
        else:
            logger.error(
                "MQTT connection failed: %s", reason_code
            )

    def on_disconnect(
        self, client, userdata, disconnect_flags,
        reason_code, properties
    ):
        if reason_code != 0:
            logger.warning(
                "Unexpected MQTT disconnection: %s",
                reason_code
            )

    def on_message(self, client, userdata, message):
        try:
            self.message_queue.put_nowait(
                (message.topic, message.payload)
            )
        except queue.Full:
            logger.error(
                "Message queue full; dropping message from %s",
                message.topic
            )

    def connect(self):
        self.client.connect(
            self.broker,
            self.port,
            keepalive=60
        )

        self.client.loop_start()

    def disconnect(self):
        self.client.disconnect()
        self.client.loop_stop()
        logger.info("MQTT client stopped")
