package com.demo.gpsspark.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Mirrors the JSON produced by gps-simulator. Needs to be a proper JavaBean
 * (public no-arg constructor + getters/setters) so Spark's Encoders.bean(...)
 * can build a Dataset<GpsPing> via reflection.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GpsPing implements Serializable {
    private String carId;
    private double latitude;
    private double longitude;
    private double speedKmh;
    private double heading;
    private Timestamp timestamp;
}
