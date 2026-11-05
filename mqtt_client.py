
import paho.mqtt.client as mqtt


class MQTTClient:
    def __init__(self, broker, port, topic):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.message_callback = None

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print("Successfully connected to MQTT broker")
            client.subscribe(self.topic)
        else:
            print(f"MQTT connection failed: {reason_code}")

    def on_message(self, client, userdata, message):
        payload = message.payload.decode("utf-8")

        print(f"Received message: {payload}")

        if self.message_callback:
            self.message_callback(payload)

    def set_message_callback(self, callback):
        self.message_callback = callback

    def connect(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
