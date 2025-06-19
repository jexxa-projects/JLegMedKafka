package io.jexxa.jlegmedkafka.plugins.mykafka;

import io.jexxa.jlegmed.core.filter.FilterContext;
import io.jexxa.jlegmedkafka.plugins.esp.kafka.KafkaESPProducer;

import static java.time.Instant.now;

public class MyKafkaSender {

    public static void sendToKafkaFluent(MyKafkaTestMessage message, FilterContext filterContext)
    {
        new KafkaESPProducer<String, MyKafkaTestMessage>(filterContext.filterProperties())
                    .send("test", message)
                    .withTimestamp(now())
                    .toTopic("demo_java")
                    .asJSON();
    }

    private MyKafkaSender() {}
}
