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