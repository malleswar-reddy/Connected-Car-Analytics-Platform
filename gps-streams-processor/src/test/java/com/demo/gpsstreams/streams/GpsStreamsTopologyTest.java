package com.demo.gpsstreams.streams;

import com.demo.gpsstreams.model.GpsData;
import com.demo.gpsstreams.model.SpeedStats;
import com.demo.gpsstreams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class GpsStreamsTopologyTest {

    private final List<GpsData> rawPingsSunk = new ArrayList<>();
    private final List<SpeedStats> statsSunk = new ArrayList<>();
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, GpsData> inputTopic;

    @BeforeEach
    void setUp() {
        Topology topology = GpsStreamsTopology.build(
                "car-gps-data",
                "car-speed-stats",
                Duration.ofSeconds(30),
                Duration.ofSeconds(0),
                rawPingsSunk::add,
                statsSunk::add
        );

        Properties props = new Properties();
        props.put(org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        testDriver = new TopologyTestDriver(topology, props);
        inputTopic = testDriver.createInputTopic(
                "car-gps-data", Serdes.String().serializer(), new JsonSerde<>(GpsData.class).serializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void everyPingIsSunkToRawSink() {
        inputTopic.pipeInput("car-1", ping("car-1", 40.0, Instant.parse("2026-01-01T00:00:01Z")));
        inputTopic.pipeInput("car-1", ping("car-1", 60.0, Instant.parse("2026-01-01T00:00:02Z")));

        assertThat(rawPingsSunk).hasSize(2);
    }

    @Test
    void aggregatesAvgAndMaxSpeedWithinSameWindow() {
        // both pings fall inside the same 30s tumbling window starting at :00:00
        inputTopic.pipeInput("car-1", ping("car-1", 40.0, Instant.parse("2026-01-01T00:00:01Z")));
        inputTopic.pipeInput("car-1", ping("car-1", 60.0, Instant.parse("2026-01-01T00:00:02Z")));

        assertThat(statsSunk).isNotEmpty();
        SpeedStats latest = statsSunk.get(statsSunk.size() - 1);

        assertThat(latest.getCarId()).isEqualTo("car-1");
        assertThat(latest.getPingCount()).isEqualTo(2);
        assertThat(latest.getAvgSpeed()).isEqualTo(50.0);
        assertThat(latest.getMaxSpeed()).isEqualTo(60.0);
    }

    @Test
    void differentCarsAreAggregatedIndependently() {
        inputTopic.pipeInput("car-1", ping("car-1", 100.0, Instant.parse("2026-01-01T00:00:01Z")));
        inputTopic.pipeInput("car-2", ping("car-2", 20.0, Instant.parse("2026-01-01T00:00:01Z")));

        List<SpeedStats> car1Stats = statsSunk.stream().filter(s -> s.getCarId().equals("car-1")).toList();
        List<SpeedStats> car2Stats = statsSunk.stream().filter(s -> s.getCarId().equals("car-2")).toList();

        assertThat(car1Stats).isNotEmpty();
        assertThat(car2Stats).isNotEmpty();
        assertThat(car1Stats.get(car1Stats.size() - 1).getMaxSpeed()).isEqualTo(100.0);
        assertThat(car2Stats.get(car2Stats.size() - 1).getMaxSpeed()).isEqualTo(20.0);
    }

    @Test
    void newTumblingWindowStartsFreshAggregate() {
        // window 1: [00:00:00, 00:00:30)
        inputTopic.pipeInput("car-1", ping("car-1", 100.0, Instant.parse("2026-01-01T00:00:01Z")));
        // window 2: [00:00:30, 00:01:00)
        inputTopic.pipeInput("car-1", ping("car-1", 10.0, Instant.parse("2026-01-01T00:00:31Z")));

        assertThat(statsSunk).hasSize(2);
        assertThat(statsSunk.get(0).getMaxSpeed()).isEqualTo(100.0);
        assertThat(statsSunk.get(1).getMaxSpeed()).isEqualTo(10.0); // fresh window, not cumulative with the first
    }

    private GpsData ping(String carId, double speedKmh, Instant timestamp) {
        return new GpsData(carId, 37.0, -122.0, speedKmh, 90.0, timestamp);
    }
}
