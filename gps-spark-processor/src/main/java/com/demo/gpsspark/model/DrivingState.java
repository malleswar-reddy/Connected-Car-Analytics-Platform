package com.demo.gpsspark.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Running state for the trip a car is currently on. Kept by Spark's state
 * store, keyed by carId, and carried across micro-batches until either a
 * gap triggers a new trip or the processing-time timeout closes it out.
 */
@Data
@NoArgsConstructor
public class DrivingState implements Serializable {
    private String tripId;
    private long tripStartEpochMillis;
    private long lastEventEpochMillis;
    private double lastSpeedKmh;
    private long pingCount;
    private double sumSpeedKmh;
    private double maxSpeedKmh;
    private int harshBrakeCount;
    private int harshAccelCount;
    private int speedingCount;
}
