package com.demo.gpsstreams.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SpeedStats {
    private String carId;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant windowStart;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant windowEnd;
    private long pingCount;
    private double avgSpeed;
    private double maxSpeed;
}
