package io.jexxa.jlegmedkafka;

import io.jexxa.jlegmed.core.JLegMed;
import io.jexxa.jlegmedkafka.plugins.event.kafka.KafkaSender;

import java.util.concurrent.TimeUnit;

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
                .and().consumeWith(KafkaSender::sendToKafka);

        jLegMed.monitorPipes("HelloKafka", logFunctionStyle());
        jLegMed.run();
    }

}
