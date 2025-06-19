package io.jexxa.jlegmedkafka;

import io.jexxa.jlegmed.core.JLegMed;
import io.jexxa.jlegmedkafka.plugins.mykafka.MyKafkaProducer;
import io.jexxa.jlegmedkafka.plugins.mykafka.MyKafkaTestMessage;
import io.jexxa.jlegmedkafka.plugins.mykafka.MyKafkaTestMessage2;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.jexxa.common.facade.logger.SLF4jLogger.getLogger;
import static io.jexxa.jlegmed.plugins.monitor.LogMonitor.logFunctionStyle;

public final class JLegMedKafka
{
    public static void main(String[] args)
    {

        getLogger(JLegMedKafka.class).info("I am a Kafka Producer");
        var jLegMed = new JLegMed(JLegMedKafka.class);
        jLegMed.newFlowGraph("HelloKafka")
                .every(1, TimeUnit.SECONDS)

                .receive(String.class).from( () -> "Hello " )
                .and().processWith(data -> data + "World" )
                .and().processWith( MyKafkaTestMessage::new )
                .and().consumeWith(MyKafkaProducer::sendToKafka).useProperties("test-kafka-connection");

        AtomicInteger counter = new AtomicInteger(0);

        jLegMed.newFlowGraph("HelloKafka2")
                .every(1, TimeUnit.SECONDS)

                .receive(Integer.class).from(counter::incrementAndGet)
                .and().processWith(MyKafkaTestMessage2::new )
                .and().consumeWith(MyKafkaProducer::sendToKafka2).useProperties("test-kafka-connection");

        jLegMed.monitorPipes("HelloKafka", logFunctionStyle());
        jLegMed.run();
    }

}
