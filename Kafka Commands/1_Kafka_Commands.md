# Kafka Setup Commands & Explanations

This covers everything related to installing Kafka, creating topics, testing with producer/consumer, and setting up Kafka Connect (JDBC Sink) to write data into PostgreSQL.

---

## 1. Installing Java (Required for Kafka)

Kafka is written in Java/Scala, so a Java runtime must be installed first. We also install `wget` to download files.

```bash
sudo dnf install -y java-17-amazon-corretto wget
```

---

## 2. Downloading and Extracting Kafka

Downloads the Kafka software (version 3.9.0) and extracts it from the compressed archive so we can use its tools.

```bash
wget https://archive.apache.org/dist/kafka/3.9.0/kafka_2.13-3.9.0.tgz
tar -xzf kafka_2.13-3.9.0.tgz
cd kafka_2.13-3.9.0
```

---

## 3. Creating the Authentication Config File

Our Kafka cluster (AWS MSK) requires a username/password to connect (SASL/SCRAM). This file stores those credentials so every Kafka command-line tool can use them automatically.

```bash
cat > client-scram.properties << 'EOF'
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="iotuser" password="Aircluster@002";
EOF
```

---

## 4. Creating a Kafka Topic

A topic is like a named channel where data gets published and read from. We create `iot-events` with 3 partitions (allows parallel processing) and replication factor 3 (keeps 3 copies of data for safety).

```bash
bin/kafka-topics.sh --create --topic iot-events \
  --bootstrap-server <broker-urls> \
  --command-config client-scram.properties \
  --partitions 3 --replication-factor 3
```

To delete a topic (used when we needed to clear out bad/corrupt test messages):

```bash
bin/kafka-topics.sh --delete --topic iot-events \
  --bootstrap-server <broker-urls> \
  --command-config client-scram.properties
```

---

## 5. Testing with a Producer (Sends Messages)

A "producer" publishes messages into a topic. We used this to manually simulate IoT sensor data being sent.

```bash
bin/kafka-console-producer.sh --topic iot-events \
  --bootstrap-server <broker-urls> \
  --producer.config client-scram.properties
```

(After running, you type a message and press Enter to send it, then Ctrl+C to exit.)

---

## 6. Testing with a Consumer (Reads Messages)

A "consumer" reads messages back out of a topic — used to confirm the producer's message actually reached Kafka.

```bash
bin/kafka-console-consumer.sh --topic iot-events \
  --bootstrap-server <broker-urls> \
  --consumer.config client-scram.properties --from-beginning
```

---

## 7. Downloading the Kafka Connect JDBC Sink Plugin

Kafka Connect needs a "plugin" (driver) to know how to write data from Kafka into a database. This downloads Confluent's official JDBC Sink Connector.

```bash
mkdir -p ~/kafka_2.13-3.9.0/connect-plugins
cd ~/kafka_2.13-3.9.0/connect-plugins
wget https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/versions/10.7.4/confluentinc-kafka-connect-jdbc-10.7.4.zip
unzip confluentinc-kafka-connect-jdbc-10.7.4.zip
```

---

## 8. Configuring the Kafka Connect Worker

This file tells the Kafka Connect process how to reach the Kafka cluster, authenticate, convert message formats (JSON), and where to find the plugin we downloaded.

```bash
cd ~/kafka_2.13-3.9.0
cat > connect-standalone.properties << 'EOF'
bootstrap.servers=<broker-urls>
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="iotuser" password="Aircluster@002";
producer.security.protocol=SASL_SSL
producer.sasl.mechanism=SCRAM-SHA-512
producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="iotuser" password="Aircluster@002";
consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=SCRAM-SHA-512
consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="iotuser" password="Aircluster@002";
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true
offset.storage.file.filename=/tmp/connect.offsets
plugin.path=/home/ec2-user/kafka_2.13-3.9.0/connect-plugins
EOF
```

> **Why `schemas.enable=true`?** This means every Kafka message must include a `schema` section (describing each field's data type) plus a `payload` section (the actual values). We initially had this set to `false` and sent plain JSON, which caused the connector to crash — switching to `true` and using the schema+payload format fixed it.

---

## 9. Configuring the JDBC Sink Connector

This defines the actual data job: read from the `iot-events` Kafka topic and write each record into the `iot_events` table in PostgreSQL.

```bash
cat > sink-postgres.properties << 'EOF'
name=postgres-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=1
topics=iot-events
connection.url=jdbc:postgresql://localhost:5432/iot_db
connection.user=iotuser
connection.password=Aircluster@002
auto.create=false
insert.mode=insert
table.name.format=iot_events
pk.mode=none
EOF
```

---

## 10. Starting Kafka Connect

Launches Kafka Connect in the background (so it keeps running even after closing the terminal) and saves all logs to `connect.log` for debugging.

```bash
nohup bin/connect-standalone.sh connect-standalone.properties sink-postgres.properties > connect.log 2>&1 &
```

Check the logs anytime to see status/errors:

```bash
tail -30 connect.log
```

Stop all running Kafka Connect processes (used when restarting after a config change):

```bash
pkill -9 -f connect-standalone
```

---

## Bugs We Hit & Fixed (Kafka Side)

| Problem | Cause | Fix |
|---|---|---|
| `wget` 404 error | Download mirror no longer hosted the file | Used Apache's permanent archive URL instead |
| `Address already in use` (port 8083) | An old Kafka Connect process was still running in the background | Killed all `connect-standalone` processes before restarting |
| `JsonDeserializer` crash / unrecoverable exception | A plain-text (non-JSON) message was accidentally sent to the topic | Deleted and recreated the Kafka topic to remove the bad message |
| `Sink connector requires... Struct value... found HashMap` | `schemas.enable=true` but message sent was plain JSON without a schema wrapper | Sent messages using the proper `{"schema":..., "payload":...}` format |
