package com.demo.gpsspark.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TripEvent implements Serializable {
    private String tripId;
    private String carId;
    private Timestamp tripStart;
    private Timestamp tripEnd;
    private String status;          // "OPEN" (still accumulating) or "CLOSED" (gap/timeout ended it)
    private long pingCount;
    private double avgSpeedKmh;
    private double maxSpeedKmh;
    private int harshBrakeCount;
    private int harshAccelCount;
    private int speedingCount;
}
