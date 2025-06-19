package io.jexxa.jlegmedkafka.plugins.esp.kafka;

import io.jexxa.jlegmed.core.filter.FilterProperties;
import io.jexxa.jlegmedkafka.plugins.esp.ESPProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import static io.jexxa.jlegmedkafka.plugins.esp.kafka.KafkaPool.kafkaProducer;
import static java.util.Objects.requireNonNull;

public class KafkaESPProducer<K,V> extends ESPProducer<K,V> {
    private final FilterProperties filterProperties;

    public static <K, V> KafkaESPProducer<K,V> kafkaESPProducer(Class<K> keyClazz,
                                                                Class<V> valueClazz,
                                                                FilterProperties filterProperties)
    {
        requireNonNull(keyClazz);
        requireNonNull(valueClazz);
        return new KafkaESPProducer<>(filterProperties);
    }

    protected KafkaESPProducer(FilterProperties filterProperties) {
        this.filterProperties = filterProperties;
    }

    @Override
    protected void sendAsJSON(K key, V eventData, String topic, Long timestamp) {
        setSerializerIfAbsent(JSONSerializer.class);
        send(key, eventData, topic, timestamp);
    }

    @Override
    protected void sendAsAVRO(K key, V eventData, String topic, Long timestamp) {
        //Not implemented yet
    }

    @Override
    protected void sendAsText(K key, V eventData, String topic, Long timestamp) {
        setSerializerIfAbsent(StringSerializer.class);
        send(key, eventData, topic, timestamp);
    }

    private <T> void setSerializerIfAbsent(Class<T> clazz)
    {
        this.filterProperties.properties().setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                filterProperties.properties().getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, clazz.getName())
        );

        this.filterProperties.properties().setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                filterProperties.properties().getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, clazz.getName())
        );
    }

    private void send(K key, V eventData, String topic, Long timestamp)
    {
        var producer = kafkaProducer(filterProperties.properties());
        producer.send(new ProducerRecord<>(topic, null, timestamp, key, eventData));
        producer.flush();
    }

}
