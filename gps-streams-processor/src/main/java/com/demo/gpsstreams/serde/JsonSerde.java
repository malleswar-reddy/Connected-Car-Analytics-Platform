package com.demo.gpsstreams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Small Jackson-backed Serde so the topology doesn't need spring-kafka or a
 * schema registry for this demo. Swap for Avro/Protobuf + Schema Registry
 * once more than one team owns these topics.
 */
public class JsonSerde<T> implements Serde<T> {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final Class<T> targetType;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing JSON for topic " + topic, e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return objectMapper.readValue(bytes, targetType);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing JSON for topic " + topic, e);
            }
        };
    }
}
