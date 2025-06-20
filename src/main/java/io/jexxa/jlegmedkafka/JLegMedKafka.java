package io.jexxa.jlegmedkafka;

import io.jexxa.jlegmed.core.JLegMed;
import io.jexxa.jlegmedkafka.plugins.mykafka.MyKafkaProducer;
import io.jexxa.jlegmedkafka.plugins.mykafka.MyKafkaTestMessage;
import io.jexxa.jlegmedkafka.plugins.mykafka.MyKafkaTestMessage2;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.jexxa.common.facade.logger.SLF4jLogger.getLogger;
import static io.jexxa.jlegmed.plugins.monitor.LogMonitor.logFunctionStyle;

public final class JLegMedKafka
{
    private static final String KAFKA_CONNECTION_PREFIX = "test-kafka-connection";

    public static void main(String[] args)
    {


        getLogger(JLegMedKafka.class).info("I am a Kafka Producer");
        var jLegMed = new JLegMed(JLegMedKafka.class);
        jLegMed.newFlowGraph("HelloKafkaAsJSON")
                .every(1, TimeUnit.SECONDS)

                .receive(String.class).from( () -> "Hello " )
                .and().processWith(data -> data + "World" )
                .and().processWith( MyKafkaTestMessage::new )
                .and().consumeWith(MyKafkaProducer::sendToKafka).useProperties(KAFKA_CONNECTION_PREFIX);

        AtomicInteger counter = new AtomicInteger(0);

        jLegMed.newFlowGraph("HelloKafkaAsJSON2")
                .every(1, TimeUnit.SECONDS)

                .receive(Integer.class).from(counter::incrementAndGet)
                .and().processWith(value -> new MyKafkaTestMessage2(value, Instant.now()) )
                .and().consumeWith(MyKafkaProducer::sendToKafka2).useProperties(KAFKA_CONNECTION_PREFIX);

      /* TODO: Still not working
      jLegMed.newFlowGraph("HelloKafkaAsText")
                .every(1, TimeUnit.SECONDS)

                .receive(String.class).from( () -> "Hello " )
                .and().processWith(data -> data + "World" )
                .and().processWith( MyKafkaTestMessage::new )
                .and().consumeWith(MyKafkaProducer::sendToKafkaAsText).useProperties(KAFKA_CONNECTION_PREFIX);
*/
        jLegMed.monitorPipes("HelloKafkaAsJSON", logFunctionStyle());
        jLegMed.run();
    }

}
