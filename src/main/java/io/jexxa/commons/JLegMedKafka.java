package io.jexxa.commons;

import io.jexxa.jlegmed.core.JLegMed;
import java.util.concurrent.TimeUnit;
import static org.slf4j.LoggerFactory.getLogger;

public final class JLegMedKafka
{
    public static void main(String[] args)
    {
        var jLegMed = new JLegMed(JLegMedKafka.class);

        jLegMed.newFlowGraph("HelloWorld")
                .every(1, TimeUnit.SECONDS)

                .receive(String.class).from( () -> "Hello " )
                .and().processWith(data -> data + "World" )
                .and().consumeWith(data -> getLogger(JLegMedKafka.class).info(data));

        jLegMed.run();
    }
}
