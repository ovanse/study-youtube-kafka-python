import json

from confluent_kafka import Consumer

# group.id - Обзываем как-то сами. Это будет идентификатор группы консюмеров. Потом, если будем ещё скелить приложение, то все потребители будут иметь один и тот же group.id
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

# Один консьюмер может подписываться на несколько топиков, поэтому имена топиков передаются как список.
consumer.subscribe(["orders"])

print("🟢 Consumer is running and subscribed to orders topic")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Error: ", msg.error())
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(
            f"📦 Received order: {order['quantity']} x {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")
finally:
    consumer.close()
