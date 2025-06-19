package io.jexxa.jlegmedkafka.plugins.esp.kafka;


import io.jexxa.common.facade.json.JSONManager;
import org.apache.kafka.common.serialization.Serializer;

public class  JSONSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        return JSONManager.getJSONConverter().toJson(data).getBytes();
    }
}
