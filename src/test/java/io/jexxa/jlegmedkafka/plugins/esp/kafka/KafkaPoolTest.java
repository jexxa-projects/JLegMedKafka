package io.jexxa.jlegmedkafka.plugins.esp.kafka;

import io.jexxa.jlegmed.core.BootstrapRegistry;
import io.jexxa.jlegmed.core.FailFastException;
import io.jexxa.jlegmed.core.JLegMed;
import io.jexxa.jlegmed.core.filter.FilterProperties;
import io.jexxa.jlegmedkafka.digispine.DigiSpine;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaPoolTest {

    static final DigiSpine DIGI_SPINE = new DigiSpine();


    @AfterAll
    static void stopKafka() {
        DIGI_SPINE.stop();
    }

    @Test
    void failFastSuccess()
    {
        //Arrange
        var filterProperties = new FilterProperties("Test", DIGI_SPINE.kafkaProperties());

        @SuppressWarnings("unused") // We need this for proper initialization of KafkaPool
        var jLegMed = new JLegMed(KafkaPoolTest.class)
                .useTechnology(KafkaPool.class);

        //Act / Assert
        assertDoesNotThrow(() -> BootstrapRegistry.initFailFast(filterProperties));
    }

    @Test
    void failFastFailure()
    {
        //Arrange
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:12345");
        var filterProperties = new FilterProperties("Test", consumerProps);

        @SuppressWarnings("unused") // We need this for proper initialization of KafkaPool
        var jLegMed = new JLegMed(KafkaPoolTest.class)
                .useTechnology(KafkaPool.class);
        //Act / Assert
        assertThrows(FailFastException.class, () -> BootstrapRegistry.initFailFast(filterProperties));
    }
}