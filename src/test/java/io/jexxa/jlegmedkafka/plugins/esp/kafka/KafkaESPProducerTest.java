package io.jexxa.jlegmedkafka.plugins.esp.kafka;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static io.jexxa.jlegmed.core.filter.FilterProperties.filterPropertiesOf;
import static io.jexxa.jlegmedkafka.plugins.esp.kafka.KafkaESPProducer.kafkaESPProducer;
import static java.time.Instant.now;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaESPProducerTest {
    static KafkaContainer kafkaBroker;
    static GenericContainer<?> schemaRegistry;
    private static Properties kafkaProperties;

    private static final Network NETWORK = Network.newNetwork();
    private static final String TEST_TEXT_TOPIC = "test-text-topic";
    private static final String TEST_JSON_TOPIC = "test-json-topic";



    @BeforeAll
    static void startKafka() {
        kafkaBroker = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:8.0.0"))
                .withNetwork(NETWORK)
                .withNetworkAliases("kafka")
                .withKraft();

        kafkaBroker.start();

        schemaRegistry = new GenericContainer<>(
                DockerImageName.parse("confluentinc/cp-schema-registry:8.0.0"))
                .withNetwork(NETWORK)
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withExposedPorts(8081)
                .waitingFor(Wait.forHttp("/subjects"));
        schemaRegistry.start();

        var schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
        kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers());
        kafkaProperties.put("schema.registry.url", schemaRegistryUrl);
    }

    @BeforeEach
    void deleteTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers()))) {
            if (admin.listTopics().names().get().contains(TEST_TEXT_TOPIC)) {
                admin.deleteTopics(Collections.singletonList(TEST_TEXT_TOPIC)).all().get();
            }
            if (admin.listTopics().names().get().contains(TEST_JSON_TOPIC)) {
                admin.deleteTopics(Collections.singletonList(TEST_JSON_TOPIC)).all().get();
            }

            NewTopic textTopic = new NewTopic(TEST_TEXT_TOPIC, 1, (short) 1);
            NewTopic jsonTopic = new NewTopic(TEST_JSON_TOPIC, 1, (short) 1);
            admin.createTopics(Arrays.asList(textTopic, jsonTopic)).all().get();
        }
    }

    @AfterAll
    static void stopKafka() {
        schemaRegistry.stop();
        kafkaBroker.stop();
    }

    @Test
    void sendAsJSON() {

            //Arrange
            var expectedResult = new KafkaTestMessage(1, Instant.now(), "test message");
            Properties consumerProperties = new Properties();
            consumerProperties.putAll(kafkaProperties);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
            consumerProperties.put("json.value.type", KafkaTestMessage.class.getName());
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "sendAsJSON");
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            var objectUnderTest = kafkaESPProducer( String.class,
                    KafkaTestMessage.class,
                    filterPropertiesOf("sendAsJSON", kafkaProperties));

            //Act
            objectUnderTest
                    .send("test", expectedResult)
                    .withTimestamp(now())
                    .toTopic(TEST_JSON_TOPIC)
                    .asJSON();

            KafkaTestMessage result = receiveGenericMessage(consumerProperties, TEST_JSON_TOPIC);

            assertEquals(expectedResult, result);
    }


    @Test
    void sendAsText() {
        // Arrange
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(kafkaProperties);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "sendAsText");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var objectUnderTest = kafkaESPProducer( String.class,
                KafkaTestMessage.class,
                filterPropertiesOf("objectUnderTest", kafkaProperties));

        var expectedResult = new KafkaTestMessage(1, Instant.now(), "test message");

        //Act
        objectUnderTest
                .send("test", expectedResult)
                .withTimestamp(now())
                .toTopic(TEST_TEXT_TOPIC)
                .asText();

        String result = receiveTextMessage(consumerProperties, TEST_TEXT_TOPIC);

        assertEquals(expectedResult.toString(), result);
    }


    private static String receiveTextMessage(Properties consumerProps, String topic)
    {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records.iterator().next().value();
            }
        }
        return null;
    }

    private static <T> T receiveGenericMessage(Properties consumerProps, String topic)
    {
        try (KafkaConsumer<String, T> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records.iterator().next().value();
            }
        }
        return null;
    }

    public record KafkaTestMessage(int counter, Instant timestamp, String message) { }

}