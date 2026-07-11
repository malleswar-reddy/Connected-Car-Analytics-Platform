package com.demo.gpsstreams.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GpsData {
    private String carId;
    private double latitude;
    private double longitude;
    private double speedKmh;
    private double heading;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;
}
