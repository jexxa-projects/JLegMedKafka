package io.jexxa.jlegmedkafka.plugins.esp;

import io.jexxa.common.facade.utils.annotation.CheckReturnValue;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

public class ESPBuilder<K,V> {
    private final K key;
    private final V event;
    private final ESPProducer<K,V> espProducer;

    private Long timestamp = null;
    private String topic;

    protected ESPBuilder(V event, ESPProducer<K,V> espProducer){
        this(null, event, espProducer);
    }

    protected ESPBuilder(K key, V event, ESPProducer<K,V> espProducer){
        this.key = key;
        this.event = event;
        this.espProducer = espProducer;
    }

    @CheckReturnValue
    public ESPBuilder<K,V> withTimestamp(Instant timestamp){
        this.timestamp = timestamp.toEpochMilli();
        return this;
    }

    @CheckReturnValue
    public ESPBuilder<K,V> toTopic(String topic){
        this.topic = requireNonNull(topic);
        return this;
    }

    public void asJSON(){
        espProducer.sendAsJSON(key,
                event,
                requireNonNull(topic),
                timestamp);
    }

    public void asAVRO(){
        espProducer.sendAsAVRO(key,
                event,
                requireNonNull(topic),
                timestamp);
    }

    public void asText(){
        espProducer.sendAsText(key,
                event,
                requireNonNull(topic),
                timestamp);
    }
}
