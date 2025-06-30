package io.jexxa.jlegmedkafka.plugins.esp.kafka;

import io.jexxa.jlegmed.core.filter.FilterProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import static io.jexxa.jlegmedkafka.plugins.esp.kafka.KafkaESPProducer.kafkaESPProducer;
import static java.time.Instant.now;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaESPProducerTest {

    static ConfluentKafkaContainer kafkaBroker;
    static GenericContainer<?> schemaRegistry;
    static String schemaRegistryUrl;
    private static final Network NETWORK = Network.newNetwork();
    private static final String TEST_TOPIC = "test-topic";


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

    @BeforeEach
    void setup() throws Exception {
        try (AdminClient admin = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers()))) {
            if (admin.listTopics().names().get().contains(TEST_TOPIC)) {
                admin.deleteTopics(Collections.singletonList(TEST_TOPIC)).all().get();
            }
            NewTopic topic = new NewTopic(TEST_TOPIC, 1, (short) 1);
            admin.createTopics(Collections.singletonList(topic)).all().get();
        }
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
                .toTopic(TEST_TOPIC)
                .asJSON());
    }

    @Test
    void sendAsText() {
        //Arrange
        var expectedResult = new KafkaTestMessage(1, Instant.now(), "test message");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sendAsText");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var filterProperties = new FilterProperties("Test", properties);

        var objectUnderTest = kafkaESPProducer( String.class, KafkaTestMessage.class, filterProperties);

        //Act
        objectUnderTest
                .send("test", expectedResult)
                .withTimestamp(now())
                .toTopic(TEST_TOPIC)
                .asText();

        String result = receiveMessage(properties);

        assertEquals(expectedResult.toString(), result);
    }


    private static <T> T receiveMessage(Properties consumerProps)
    {
        try (KafkaConsumer<String, T> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(TEST_TOPIC));
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records.iterator().next().value();
            }
        }
        return null;
    }

    public record KafkaTestMessage(int counter, Instant timestamp, String message) { }

}