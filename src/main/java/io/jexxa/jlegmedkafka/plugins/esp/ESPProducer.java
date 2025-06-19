package io.jexxa.jlegmedkafka.plugins.esp;

import io.jexxa.common.facade.utils.annotation.CheckReturnValue;

public abstract class ESPProducer<K,V> {

    @CheckReturnValue
    public ESPBuilder<K,V> send(V eventData){
        return new ESPBuilder<>(eventData, this);
    }

    @CheckReturnValue
    public ESPBuilder<K,V> send(K key, V eventData){
        return new ESPBuilder<>(key, eventData, this);
    }

    protected abstract void sendAsJSON(K key, V eventData, String topic, Long timestamp);

    protected abstract void sendAsAVRO(K key, V eventData, String topic, Long timestamp);

    protected abstract void sendAsText(K key, V eventData, String topic, Long timestamp);
}
