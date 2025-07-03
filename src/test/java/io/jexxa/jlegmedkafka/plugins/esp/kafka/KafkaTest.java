package io.jexxa.jlegmedkafka.plugins.esp.kafka;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.time.Duration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.util.Collections;

public class KafkaTest {

    @Test
    public void testKafkaWithSchemaRegistry() {
        Network network = Network.newNetwork();
        try (
                KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1")).withNetwork( network).withNetworkAliases("kafka")
        ) {
            System.out.println(">Kafka start");

            kafka.start();
            System.out.println("<Kafka started");

            GenericContainer<?> schemaRegistry = new GenericContainer<>(
                    DockerImageName.parse("confluentinc/cp-schema-registry:7.5.1"))
                    .withNetwork(network)
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withExposedPorts(8081)
                    .waitingFor(Wait.forHttp("/subjects"));

            System.out.println(">Schemaregistry start");

            schemaRegistry.start();

            System.out.println("Schemaregistry started");

            String schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
            String topic = "test-topic";

/*

            // Producer properties
            Properties producerProps = new Properties();
            producerProps.put("bootstrap.servers", kafka.getBootstrapServers());
            producerProps.put("key.serializer", KafkaAvroSerializer.class.getName());
            producerProps.put("value.serializer", KafkaAvroSerializer.class.getName());
            producerProps.put("schema.registry.url", schemaRegistryUrl);

            // Simple Avro record
            String schemaString = "{"
                    + "\"type\":\"record\","
                    + "\"name\":\"User\","
                    + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]"
                    + "}";

            org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaString);
            org.apache.avro.generic.GenericRecord record = new org.apache.avro.generic.GenericData.Record(schema);
            record.put("name", "Max Mustermann");

            try (KafkaProducer<Object, Object> producer = new KafkaProducer<>(producerProps)) {
                producer.send(new ProducerRecord<>(topic, "key1", record));
                producer.flush();
            }

            // Consumer properties
            Properties consumerProps = new Properties();
            consumerProps.put("bootstrap.servers", kafka.getBootstrapServers());
            consumerProps.put("group.id", "test-group");
            consumerProps.put("key.deserializer", KafkaAvroDeserializer.class.getName());
            consumerProps.put("value.deserializer", KafkaAvroDeserializer.class.getName());
            consumerProps.put("schema.registry.url", schemaRegistryUrl);
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put("specific.avro.reader", false);

            try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singleton(topic));
                ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofSeconds(5));
                records.forEach(rec -> {
                    System.out.println("Received record: key=" + rec.key() + ", value=" + rec.value());
                });
            }
*/
            kafka.stop();
            schemaRegistry.stop();
        }
    }
}