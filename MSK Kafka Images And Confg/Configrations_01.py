cat > connect-standalone.properties << 'EOF'
bootstrap.servers=boot-z2e.iotclusterpro.16w0x0.c24.kafka.us-east-1.amazonaws.com:9096,boot-i1n.iotclusterpro.16w0x0.c24.kafka.us-east-1.amazonaws.com:9096,boot-lhe.iotclusterpro.16w0x0.c24.kafka.us-east-1.amazonaws.com:9096
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
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.file.filename=/tmp/connect.offsets
plugin.path=/home/ec2-user/kafka_2.13-3.9.0/connect-plugins
EOF