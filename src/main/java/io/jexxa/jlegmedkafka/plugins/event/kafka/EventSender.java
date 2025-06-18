package io.jexxa.jlegmedkafka.plugins.event.kafka;

import io.jexxa.common.facade.utils.annotation.CheckReturnValue;

public abstract class EventSender<K,V> {

    @CheckReturnValue
    public EventBuilder<K,V> send(V eventData){
        return new EventBuilder<>(eventData, this);
    }

    @CheckReturnValue
    public EventBuilder<K,V> send(K key, V eventData){
        return new EventBuilder<>(key, eventData, this);
    }

    protected abstract void sendAsJSON(K key, V eventData, String topic, Long timestamp);

    protected abstract void sendAsAVRO(K key, V eventData, String topic, Long timestamp);
}
