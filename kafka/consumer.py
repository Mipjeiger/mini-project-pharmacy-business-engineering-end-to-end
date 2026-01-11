import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError


def create_consumer(group_id="pharmacy_sales_group"):
    """Create a new Kafka consumer instance"""
    try:
        consumer = KafkaConsumer(
            "pharmacy_sales",
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id=group_id,  # Make this configurable
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            session_timeout_ms=30000,
            max_poll_records=500,
        )
        print(f"✓ Kafka consumer created successfully (group: {group_id})")
        return consumer
    except KafkaError as e:
        print(f"✗ Failed to create Kafka consumer: {e}")
        raise


def read_batch(
    limit=1000, consumer=None, max_batches=None, group_id="pharmacy_sales_group"
):
    """
    Read messages from Kafka in batches

    Args:
        limit: Maximum messages per batch
        consumer: Optional existing consumer instance. If None, creates new one.
        max_batches: Maximum number of batches to yield before stopping
        group_id: Consumer group ID

    Yields:
        List of messages (batch)
    """
    # Create consumer if not provided
    should_close = False
    if consumer is None:
        consumer = create_consumer(group_id=group_id)
        should_close = True

    try:
        message = []
        batch_count = 0
        empty_polls = 0
        max_empty_polls = 3

        print(
            f"📥 Starting to poll messages (limit={limit}, max_batches={max_batches})..."
        )

        while True:
            try:
                msg_pack = consumer.poll(timeout_ms=2000)

                if msg_pack:
                    empty_polls = 0

                    for tp, messages in msg_pack.items():
                        print(
                            f"  ✓ Received {len(messages)} messages from partition {tp.partition}"
                        )

                        for msg in messages:
                            message.append(msg.value)

                            if len(message) >= limit:
                                print(
                                    f"  📦 Yielding batch #{batch_count + 1} with {len(message)} messages"
                                )
                                yield message
                                message = []
                                batch_count += 1

                                if max_batches and batch_count >= max_batches:
                                    print(
                                        f"  ✓ Reached max_batches limit ({max_batches})"
                                    )
                                    if message:
                                        print(
                                            f"  📦 Yielding final batch with {len(message)} messages"
                                        )
                                        yield message
                                    return
                else:
                    empty_polls += 1
                    print(
                        f"  ⏳ No messages (empty poll {empty_polls}/{max_empty_polls})"
                    )

                if not msg_pack and message:
                    print(
                        f"  📦 Yielding partial batch #{batch_count + 1} with {len(message)} messages"
                    )
                    yield message
                    message = []
                    batch_count += 1

                    if max_batches and batch_count >= max_batches:
                        print(f"  ✓ Reached max_batches limit ({max_batches})")
                        return

                if empty_polls >= max_empty_polls:
                    print(f"  ⏹ Stopping after {max_empty_polls} empty polls")
                    if message:
                        print(f"  📦 Yielding final batch with {len(message)} messages")
                        yield message
                    return

            except KafkaError as e:
                print(f"  ✗ Error during poll: {e}")
                time.sleep(1)
                continue

    finally:
        if should_close:
            print("🔒 Closing consumer...")
            consumer.close()
            print("✓ Consumer closed")


if __name__ == "__main__":
    print("=" * 60)
    print("Starting Kafka Consumer Test")
    print("=" * 60)
    print("Broker: localhost:9092")
    print("Topic: pharmacy_sales")
    print("Group: pharmacy_sales_group")
    print("-" * 60)

    try:
        total_messages = 0
        batch_num = 0

        for batch in read_batch(limit=500, max_batches=10):
            batch_num += 1
            total_messages += len(batch)
            print(f"\n📊 Batch #{batch_num}: {len(batch)} messages")

            if batch_num == 1 and batch:
                print(f"   Sample message:")
                sample = batch[0]
                for key in [
                    "distributor",
                    "city",
                    "product_name",
                    "sales",
                    "month",
                    "year",
                ]:
                    if key in sample:
                        print(f"     - {key}: {sample[key]}")

        print("\n" + "=" * 60)
        print(f"✅ Consumption complete!")
        print(f"   Total batches: {batch_num}")
        print(f"   Total messages: {total_messages}")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\n⚠️  Stopping consumer (Ctrl+C)...")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback

        traceback.print_exc()

    print("\n✓ Done!")
