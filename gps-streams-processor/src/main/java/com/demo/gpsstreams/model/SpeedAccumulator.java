package com.demo.gpsstreams.model;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Running accumulator held inside the KTable state store while a window is open.
 * Kept intentionally mutable/simple since Kafka Streams calls the Aggregator
 * repeatedly for the same key within a window.
 */
@Data
@NoArgsConstructor
public class SpeedAccumulator {
    private long count = 0;
    private double sumSpeed = 0.0;
    private double maxSpeed = 0.0;

    public SpeedAccumulator add(GpsData ping) {
        this.count += 1;
        this.sumSpeed += ping.getSpeedKmh();
        this.maxSpeed = Math.max(this.maxSpeed, ping.getSpeedKmh());
        return this;
    }

    public double avgSpeed() {
        return count == 0 ? 0.0 : sumSpeed / count;
    }
}
