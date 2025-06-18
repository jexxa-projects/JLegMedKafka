package io.jexxa.jlegmedkafka.plugins.event.kafka;

import io.jexxa.common.facade.utils.annotation.CheckReturnValue;

import java.time.Instant;
import java.util.Objects;

public class EventBuilder<K,V> {
    private final K key;
    private final V eventData;
    private final EventSender<K,V> eventSender;

    private Long timestamp = null;
    private String topic;

    protected EventBuilder(V eventData, EventSender<K,V> eventSender){
        this(null, eventData, eventSender);
    }

    protected  EventBuilder(K key, V eventData, EventSender<K,V> eventSender){
        this.key = key;
        this.eventData = eventData;
        this.eventSender = eventSender;
    }

    @CheckReturnValue
    public EventBuilder<K,V> withTimestamp(Instant timestamp){
        this.timestamp = timestamp.toEpochMilli();
        return this;
    }

    @CheckReturnValue
    public EventBuilder<K,V> toTopic(String topic){
        this.topic = topic;
        return this;
    }

    public void asJSON(){
        Objects.requireNonNull(topic, "No topic defined");
        eventSender.sendAsJSON(key, eventData, topic, timestamp);
    }

    public void asAVRO(){}
}
