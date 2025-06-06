package io.jexxa.jlegmedkafka.plugins.event.kafka;

import io.jexxa.adapterapi.JexxaContext;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaPool {
    @SuppressWarnings("unused")
    private static final KafkaPool INSTANCE = new KafkaPool();
    record ProducerEntry(Properties properties, Class<?> keyClazz, Class<?> valueClass){}

    private static final Map<ProducerEntry, KafkaProducer<?,?>> producerMap = Collections.synchronizedMap(new ConcurrentHashMap<>());

    public static <K, V> KafkaProducer<K,V> kafkaProducer(Properties properties, Class<K> keyClazz, Class<V> valueClazz)
    {
        return (KafkaProducer<K, V>) producerMap.computeIfAbsent(new ProducerEntry(properties, keyClazz, valueClazz), producerEntry -> new KafkaProducer<>(properties));
    }

    private void cleanup()
    {
        producerMap.values().forEach(KafkaProducer::flush);
        producerMap.values().forEach(KafkaProducer::close);
        producerMap.clear();
    }

    private KafkaPool()
    {
        JexxaContext.registerCleanupHandler(this::cleanup);
    }
}
