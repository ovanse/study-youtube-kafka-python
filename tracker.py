from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",  # Обзываем как-то сами. Это будет идентификатор группы консюмеров. Потом, если будем ещё скелить приложение, то все потребители будут иметь один и тот же group.id
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
