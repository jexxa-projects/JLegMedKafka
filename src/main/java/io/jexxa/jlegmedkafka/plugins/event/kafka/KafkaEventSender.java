package io.jexxa.jlegmedkafka.plugins.event.kafka;

import io.jexxa.jlegmed.core.filter.FilterProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static io.jexxa.jlegmedkafka.plugins.event.kafka.KafkaPool.kafkaProducer;

public class KafkaEventSender<K,V> extends EventSender<K,V> implements AutoCloseable {
        private final FilterProperties filterProperties;

        public KafkaEventSender(final FilterProperties filterProperties) {
            this.filterProperties = filterProperties;
        }

        @Override
        protected void sendAsJSON(K key, V eventData, String topic, Long timestamp) {
            KafkaProducer<K,V> producer = kafkaProducer(filterProperties.properties(), key.getClass(), eventData.getClass());
            producer.send(new ProducerRecord<K,V>(topic, null, timestamp, key, eventData));
            producer.flush();

        }

        @Override
        protected void sendAsAVRO(K key, V eventData, String topic, Long timestamp) {

        }

        @Override
        public void close() throws Exception {

        }
}
