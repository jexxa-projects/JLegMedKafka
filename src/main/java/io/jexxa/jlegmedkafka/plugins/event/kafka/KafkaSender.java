package io.jexxa.jlegmedkafka.plugins.event.kafka;

import io.jexxa.jlegmed.core.filter.FilterContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import static io.jexxa.jlegmedkafka.plugins.event.kafka.KafkaPool.kafkaProducer;
import static java.time.Instant.now;

public class KafkaSender {

    public static void sendToKafka(KafkaTestMessage message, FilterContext filterContext) {

        // create Producer properties
        filterContext.properties().setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JSONSerializer.class.getName());
        filterContext.properties().setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerializer.class.getName());

        // create the producer
        KafkaProducer<String, KafkaTestMessage> producer = kafkaProducer(filterContext.properties(), String.class, KafkaTestMessage.class);

        // create a producer record
        ProducerRecord<String, KafkaTestMessage> producerRecord =
                new ProducerRecord<>("demo_java", message);

        // send data - asynchronous
        producer.send(producerRecord);
    }

    public static void sendToKafkaFluent(KafkaTestMessage message, FilterContext filterContext)
    {        filterContext.properties().setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JSONSerializer.class.getName());
        filterContext.properties().setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerializer.class.getName());

        new KafkaEventSender<String, KafkaTestMessage>(filterContext.filterProperties())
                    .send("test", message)
                    .withTimestamp(now())
                    .toTopic("demo_java")
                    .asJSON();
    }

    private KafkaSender() {}
}
