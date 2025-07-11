package io.jexxa.jlegmedkafka.plugins.mykafka;

import io.jexxa.jlegmed.core.filter.FilterContext;

import static io.jexxa.esp.drivenadapter.kafka.KafkaESPProducer.kafkaESPProducer;
import static java.time.Instant.now;

public class MyKafkaProducer {

    public static void sendToKafka(MyKafkaTestMessage message, FilterContext filterContext)
    {
        kafkaESPProducer(String.class, MyKafkaTestMessage.class, filterContext.filterProperties().properties())
                    .send("test", message)
                    .withTimestamp(now())
                    .toTopic("demo_java_json")
                    .asJSON();
    }

    public static void sendToKafka2(MyKafkaTestMessage2 message, FilterContext filterContext)
    {
        kafkaESPProducer(String.class, MyKafkaTestMessage2.class, filterContext.properties())
                .send("test2", message)
                .withTimestamp(now())
                .toTopic("demo_java_json2")
                .asJSON();
    }

    public static void sendToKafkaAsText(MyKafkaTestMessage message, FilterContext filterContext)
    {
        kafkaESPProducer(String.class, MyKafkaTestMessage.class, filterContext.properties())
                .send("test", message)
                .withTimestamp(now())
                .toTopic("demo_java_text")
                .asText();
    }

    private MyKafkaProducer() {}
}
