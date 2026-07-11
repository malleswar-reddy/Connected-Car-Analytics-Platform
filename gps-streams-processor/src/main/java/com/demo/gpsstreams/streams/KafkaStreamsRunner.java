package com.demo.gpsstreams.streams;

import com.demo.gpsstreams.repository.GpsPingRepository;
import com.demo.gpsstreams.repository.SpeedStatsRepository;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;

@Slf4j
@Component
public class KafkaStreamsRunner {

    private final GpsPingRepository gpsPingRepository;
    private final SpeedStatsRepository speedStatsRepository;

    private final String bootstrapServers;
    private final String applicationId;
    private final String inputTopic;
    private final String statsOutputTopic;
    private final Duration windowSize;
    private final Duration windowGrace;

    private KafkaStreams streams;

    public KafkaStreamsRunner(GpsPingRepository gpsPingRepository,
                              SpeedStatsRepository speedStatsRepository,
                              @Value("${gps.bootstrap-servers}") String bootstrapServers,
                              @Value("${gps.application-id}") String applicationId,
                              @Value("${gps.input-topic}") String inputTopic,
                              @Value("${gps.stats-output-topic}") String statsOutputTopic,
                              @Value("${gps.window-size-seconds}") long windowSizeSeconds,
                              @Value("${gps.window-grace-seconds}") long windowGraceSeconds) {
        this.gpsPingRepository = gpsPingRepository;
        this.speedStatsRepository = speedStatsRepository;
        this.bootstrapServers = bootstrapServers;
        this.applicationId = applicationId;
        this.inputTopic = inputTopic;
        this.statsOutputTopic = statsOutputTopic;
        this.windowSize = Duration.ofSeconds(windowSizeSeconds);
        this.windowGrace = Duration.ofSeconds(windowGraceSeconds);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        Topology topology = GpsStreamsTopology.build(
                inputTopic,
                statsOutputTopic,
                windowSize,
                windowGrace,
                gpsPingRepository::insert,
                speedStatsRepository::upsert
        );

        log.info("Kafka Streams topology:\n{}", topology.describe());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        // exactly-once processing across the read-process-write cycle (consume ping,
        // update the aggregate state store, produce to car-speed-stats) — relevant
        // because the window aggregate would otherwise double-count on rebalance/retry.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler(throwable -> {
            log.error("Uncaught Kafka Streams exception, replacing thread", throwable);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
    }

    @PreDestroy
    public void stop() {
        if (streams != null) {
            log.info("Shutting down Kafka Streams");
            streams.close(Duration.ofSeconds(10));
        }
    }
}
