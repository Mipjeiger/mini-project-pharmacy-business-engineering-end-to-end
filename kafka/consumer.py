import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    topic="pharmacy_sales",
    bootstrap_servers="localhost:9095",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="pharmacy_sales_group",
)


def read_batch(limit=1000):
    message = []
    # use pool for not just waiting for limit but yielding remaining messages as well
    while True:
        msg_pack = consumer.poll(timeout_ms=1000)

        for tp, messages in msg_pack.items():
            for msg in messages:
                message.append(msg.value)
                if len(message) >= limit:
                    yield message
                    message = []

        # if one second passed and nothing messages were received, yield remaining messages
        if message:
            yield message
            message = []
