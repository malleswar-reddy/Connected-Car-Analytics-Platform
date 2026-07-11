package com.demo.gpsspark.dashboard;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.List;

public class DashboardModels {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Summary {
        private long totalTrips;
        private long totalCars;
        private double avgSpeedKmh;
        private long totalHarshBrakeEvents;
        private long totalHarshAccelEvents;
        private long totalSpeedingEvents;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CarRiskSummary {
        private String carId;
        private long tripCount;
        private double avgSpeedKmh;
        private double maxSpeedKmh;
        private long harshBrakeCount;
        private long harshAccelCount;
        private long speedingCount;
        /** Naive placeholder score (0-100), NOT the real insurance risk model —
         *  that's task 2. Just enough to color-code the dashboard for now. */
        private int riskScore;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TripRow {
        private String tripId;
        private String carId;
        private Timestamp tripStart;
        private Timestamp tripEnd;
        private String status;
        private long pingCount;
        private double avgSpeedKmh;
        private double maxSpeedKmh;
        private int harshBrakeCount;
        private int harshAccelCount;
        private int speedingCount;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DashboardResponse {
        private Summary summary;
        private List<CarRiskSummary> perCar;
        private List<TripRow> recentTrips;
    }
}
