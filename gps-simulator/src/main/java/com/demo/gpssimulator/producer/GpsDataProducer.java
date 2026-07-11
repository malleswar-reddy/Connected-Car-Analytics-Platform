package com.demo.gpssimulator.producer;

import com.demo.gpssimulator.model.GpsData;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
public class GpsDataProducer {

    private final Producer<String, GpsData> producer;
    private final String topic;

    public GpsDataProducer(@Value("${gps.bootstrap-servers}") String bootstrapServers,
                           @Value("${gps.topic}") String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        this.producer = new KafkaProducer<>(props);
    }

    public void send(GpsData gpsData) {
        // key by carId -> all pings for one car land on the same partition, preserving order
        ProducerRecord<String, GpsData> record = new ProducerRecord<>(topic, gpsData.getCarId(), gpsData);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to publish GPS ping for car {}", gpsData.getCarId(), exception);
            } else {
                log.debug("Published {} -> partition {} offset {}", gpsData, metadata.partition(), metadata.offset());
            }
        });
    }

    @PreDestroy
    public void close() {
        producer.flush();
        producer.close();
    }
}
