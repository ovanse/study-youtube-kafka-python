# KAFKA

Run Kafka locally with docker compose.

```sh
docker compose up -d
```

# topics

To view list of topics attach to container and run

```sh
kafka-topics --list --bootstrap-server localhost:9092
```

View events

```sh
kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
```
