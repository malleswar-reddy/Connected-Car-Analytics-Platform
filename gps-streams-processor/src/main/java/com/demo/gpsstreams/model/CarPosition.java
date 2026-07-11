package com.demo.gpsstreams.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Snapshot of a single car's most recent known position, sent both over the
 * REST snapshot endpoint and the WebSocket live feed.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CarPosition {
    private String carId;
    private double latitude;
    private double longitude;
    private double speedKmh;
    private double heading;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant eventTimestamp;
}
