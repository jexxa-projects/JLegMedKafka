package io.jexxa.jlegmedkafka.plugins.esp.kafka;

import io.jexxa.jlegmed.core.filter.FilterProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.time.Instant;
import java.util.Properties;

import static io.jexxa.jlegmedkafka.plugins.esp.kafka.KafkaESPProducer.kafkaESPProducer;
import static java.time.Instant.now;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class KafkaESPProducerTest {

    static ConfluentKafkaContainer kafkaBroker;
    static GenericContainer<?> schemaRegistry;
    static String schemaRegistryUrl;
    private static final Network NETWORK = Network.newNetwork();


    @BeforeAll
    static void startKafka() {
        kafkaBroker = new ConfluentKafkaContainer("confluentinc/cp-kafka:latest").withNetwork(NETWORK);;
        kafkaBroker.start();

       /* System.out.println(kafkaBroker.getBootstrapServers());
        schemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:latest"))
                .withNetwork(NETWORK)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                        "PLAINTEXT://" + kafkaBroker.getBootstrapServers()  )
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

        System.out.println(">Start");
        schemaRegistry.start();
        System.out.println("<Start");

        schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);*/
    }

    @AfterAll
    static void stopKafka() {
       // schemaRegistry.stop();
        kafkaBroker.stop();
    }

    @Test
    @Disabled
    void sendAsJSON() {
        //Arrange
        var testMessage = new KafkaTestMessage(1, Instant.now(), "test message");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers());
        properties.put("schema.registry.url", schemaRegistryUrl);

        var filterProperties = new FilterProperties("Test", properties);

        var objectUnderTest = kafkaESPProducer( String.class, KafkaTestMessage.class, filterProperties);

        //Act - Assert
        assertDoesNotThrow(() -> objectUnderTest
                .send("test", testMessage)
                .withTimestamp(now())
                .toTopic("demo_java_json")
                .asJSON());
    }

    @Test
    void sendAsText() {
        //Arrange
        var testMessage = new KafkaTestMessage(1, Instant.now(), "test message");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers());
        var filterProperties = new FilterProperties("Test", properties);

        var objectUnderTest = kafkaESPProducer( String.class, KafkaTestMessage.class, filterProperties);

        //Act - Assert
        assertDoesNotThrow(() -> objectUnderTest
                .send("test", testMessage)
                .withTimestamp(now())
                .toTopic("demo_java_json")
                .asText());
    }


    public record KafkaTestMessage(int counter, Instant timestamp, String message) { }

}