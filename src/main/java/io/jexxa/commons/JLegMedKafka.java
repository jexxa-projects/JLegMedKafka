package io.jexxa.commons;

import io.jexxa.jlegmed.core.JLegMed;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
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
                .and().consumeWith(JLegMedKafka::sendToKafka);

        jLegMed.monitorPipes("HelloKafka", logFunctionStyle());
        jLegMed.run();
    }


    private static void sendToKafka(String message)
    {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", message);

        // send data - asynchronous
        producer.send(producerRecord);

        // flush data - synchronous
        producer.flush();
        // flush and close producer
        producer.close();
    }

}
