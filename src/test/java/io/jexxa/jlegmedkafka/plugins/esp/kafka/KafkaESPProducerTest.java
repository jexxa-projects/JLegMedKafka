package io.jexxa.jlegmedkafka.plugins.esp.kafka;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.jexxa.esp.digispine.DigiSpine;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import static io.jexxa.esp.drivenadapter.kafka.KafkaESPProducer.kafkaESPProducer;
import static java.time.Instant.now;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaESPProducerTest {
    private static final String TEST_TEXT_TOPIC = "test-text-topic";
    private static final String TEST_JSON_TOPIC = "test-json-topic";

    private static final DigiSpine DIGI_SPINE = new DigiSpine();


    @BeforeEach
    void resetDigiSpine() {
        DIGI_SPINE.reset();
    }

    @AfterAll
    static void stopDigiSpine() {
        DIGI_SPINE.stop();
    }

    @Test
    void sendAsJSON() {

            //Arrange
            var expectedResult = new KafkaTestMessage(1, Instant.now(), "test message");
            Properties consumerProperties = DIGI_SPINE.kafkaProperties();
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
            consumerProperties.put("json.value.type", KafkaTestMessage.class.getName());
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "sendAsJSON");
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            var objectUnderTest = kafkaESPProducer( String.class,
                    KafkaTestMessage.class,
                    DIGI_SPINE.kafkaProperties());

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
        Properties consumerProperties = DIGI_SPINE.kafkaProperties();
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "sendAsText");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var objectUnderTest = kafkaESPProducer( String.class,
                KafkaTestMessage.class,
                DIGI_SPINE.kafkaProperties());

        var expectedResult = new KafkaTestMessage(1, Instant.now(), "test message");

        //Act
        objectUnderTest
                .send("test", expectedResult)
                .withTimestamp(now())
                .toTopic(TEST_TEXT_TOPIC)
                .asText();

        String result = receiveGenericMessage(consumerProperties, TEST_TEXT_TOPIC);

        assertEquals(expectedResult.toString(), result);
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