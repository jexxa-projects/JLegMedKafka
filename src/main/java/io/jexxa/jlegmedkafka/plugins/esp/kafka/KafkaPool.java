package io.jexxa.jlegmedkafka.plugins.esp.kafka;

import io.jexxa.adapterapi.JexxaContext;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaPool {
    @SuppressWarnings("unused")
    private static final KafkaPool INSTANCE = new KafkaPool();

    private static final Map<Properties, KafkaProducer<Object,Object>> producerMap = Collections.synchronizedMap(new ConcurrentHashMap<>());

    public static KafkaProducer<Object,Object> kafkaProducer(Properties properties)
    {
        return producerMap.computeIfAbsent(properties, entry -> new KafkaProducer<>(properties));
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
