package io.jexxa.jlegmedkafka.plugins.esp.kafka;

import io.jexxa.jlegmed.core.filter.FilterProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Properties;

import static io.jexxa.jlegmedkafka.plugins.esp.kafka.KafkaESPProducer.kafkaESPProducer;
import static java.time.Instant.now;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class KafkaTest {

    @Test
    void testKafkaWithSchemaRegistry() {
        Network network = Network.newNetwork();
        try (
                KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:8.0.0")).withNetwork( network).withNetworkAliases("kafka").withKraft();
                GenericContainer<?> schemaRegistry = new GenericContainer<>(
                    DockerImageName.parse("confluentinc/cp-schema-registry:8.0.0"));
        ) {
            System.out.println(">Kafka start");

            kafka.start();
            System.out.println("<Kafka started");

            schemaRegistry
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

            //Arrange
            var testMessage = new KafkaTestMessage(1, Instant.now(), "test message");
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            properties.put("schema.registry.url", schemaRegistryUrl);

            var filterProperties = new FilterProperties("Test", properties);

            var objectUnderTest = kafkaESPProducer( String.class, KafkaTestMessage.class, filterProperties);

            //Act - Assert
            assertDoesNotThrow(() -> objectUnderTest
                    .send("test", testMessage)
                    .withTimestamp(now())
                    .toTopic(topic)
                    .asJSON());
            kafka.stop();
            schemaRegistry.stop();
        }
    }




    public record KafkaTestMessage(int counter, Instant timestamp, String message) { }

}