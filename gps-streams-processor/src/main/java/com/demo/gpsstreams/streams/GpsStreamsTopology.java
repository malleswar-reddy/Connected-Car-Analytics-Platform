package com.demo.gpsstreams.streams;

import com.demo.gpsstreams.model.GpsData;
import com.demo.gpsstreams.model.SpeedAccumulator;
import com.demo.gpsstreams.model.SpeedStats;
import com.demo.gpsstreams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;

/**
 * Builds the topology in isolation from Spring so it can be exercised with
 * TopologyTestDriver without spinning up a broker or an ApplicationContext.
 * <p>
 * Pipeline:
 * car-gps-data (raw pings)
 * -> branch 1: sink every raw ping to Postgres (gps_ping)
 * -> branch 2: group by carId, tumbling time window, aggregate count/avg/max speed
 * -> sink windowed stats to Postgres (car_speed_window_stats)
 * -> also republish to an output topic (car-speed-stats) for downstream consumers
 */
public final class GpsStreamsTopology {

    private GpsStreamsTopology() {
    }

    public static Topology build(String inputTopic,
                                 String statsOutputTopic,
                                 Duration windowSize,
                                 Duration graceSecond,
                                 Consumer<GpsData> rawPingSink,
                                 Consumer<SpeedStats> statsSink) {

        StreamsBuilder builder = new StreamsBuilder();

        JsonSerde<GpsData> gpsSerde = new JsonSerde<>(GpsData.class);
        JsonSerde<SpeedStats> statsSerde = new JsonSerde<>(SpeedStats.class);
        JsonSerde<SpeedAccumulator> accumulatorSerde = new JsonSerde<>(SpeedAccumulator.class);

        KStream<String, GpsData> pings = builder.stream(
                inputTopic, Consumed.with(Serdes.String(), gpsSerde));

        // --- branch 1: persist every raw ping as-is ---
        pings.foreach((carId, ping) -> rawPingSink.accept(ping));

        // --- branch 2: tumbling-window speed aggregation per car ---
        TimeWindows windows = TimeWindows.ofSizeAndGrace(windowSize, graceSecond);

        KTable<Windowed<String>, SpeedAccumulator> windowedAgg = pings
                .groupByKey(org.apache.kafka.streams.kstream.Grouped.with(Serdes.String(), gpsSerde))
                .windowedBy(windows)
                .aggregate(
                        SpeedAccumulator::new,
                        (carId, ping, acc) -> acc.add(ping),
                        Materialized.<String, SpeedAccumulator, org.apache.kafka.streams.state.WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("car-speed-window-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accumulatorSerde)
                );

        KStream<String, SpeedStats> statsStream = windowedAgg.toStream()
                .map((windowedKey, acc) -> {
                    String carId = windowedKey.key();
                    Instant start = windowedKey.window().startTime();
                    Instant end = windowedKey.window().endTime();
                    SpeedStats stats = new SpeedStats(carId, start, end, acc.getCount(), acc.avgSpeed(), acc.getMaxSpeed());
                    return new KeyValue<>(carId, stats);
                });

        statsStream.foreach((carId, stats) -> statsSink.accept(stats));
        statsStream.to(statsOutputTopic, Produced.with(Serdes.String(), statsSerde));

        return builder.build();
    }
}
