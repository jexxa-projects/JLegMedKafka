package io.jexxa.jlegmedkafka.plugins.mykafka;

import io.jexxa.jlegmed.core.filter.FilterContext;
import static io.jexxa.jlegmedkafka.plugins.esp.kafka.KafkaESPProducer.kafkaESPProducer;

import static java.time.Instant.now;

public class MyKafkaProducer {

    public static void sendToKafka(MyKafkaTestMessage message, FilterContext filterContext)
    {
        kafkaESPProducer(String.class, MyKafkaTestMessage.class, filterContext.filterProperties())
                    .send("test", message)
                    .withTimestamp(now())
                    .toTopic("demo_java")
                    .asJSON();
    }

    public static void sendToKafka2(MyKafkaTestMessage2 message, FilterContext filterContext)
    {
        kafkaESPProducer(String.class, MyKafkaTestMessage2.class, filterContext.filterProperties())
                .send("test2", message)
                .withTimestamp(now())
                .toTopic("demo_java")
                .asJSON();
    }

    private MyKafkaProducer() {}
}
