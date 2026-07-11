package com.demo.gpsspark.dashboard;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class DashboardRepository {

    private final JdbcTemplate jdbcTemplate;

    public DashboardRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public DashboardModels.Summary fetchSummary() {
        return jdbcTemplate.queryForObject(
                """
                SELECT
                    COUNT(*)                              AS total_trips,
                    COUNT(DISTINCT car_id)                 AS total_cars,
                    COALESCE(AVG(avg_speed_kmh), 0)         AS avg_speed_kmh,
                    COALESCE(SUM(harsh_brake_count), 0)     AS total_harsh_brake,
                    COALESCE(SUM(harsh_accel_count), 0)     AS total_harsh_accel,
                    COALESCE(SUM(speeding_count), 0)        AS total_speeding
                FROM car_trip_behavior
                """,
                (rs, rowNum) -> new DashboardModels.Summary(
                        rs.getLong("total_trips"),
                        rs.getLong("total_cars"),
                        rs.getDouble("avg_speed_kmh"),
                        rs.getLong("total_harsh_brake"),
                        rs.getLong("total_harsh_accel"),
                        rs.getLong("total_speeding")
                )
        );
    }

    public List<DashboardModels.CarRiskSummary> fetchPerCar() {
        return jdbcTemplate.query(
                """
                SELECT
                    car_id,
                    COUNT(*)                          AS trip_count,
                    COALESCE(AVG(avg_speed_kmh), 0)    AS avg_speed_kmh,
                    COALESCE(MAX(max_speed_kmh), 0)    AS max_speed_kmh,
                    COALESCE(SUM(harsh_brake_count),0) AS harsh_brake_count,
                    COALESCE(SUM(harsh_accel_count),0) AS harsh_accel_count,
                    COALESCE(SUM(speeding_count),0)    AS speeding_count
                FROM car_trip_behavior
                GROUP BY car_id
                ORDER BY car_id
                """,
                (rs, rowNum) -> {
                    long harshBrake = rs.getLong("harsh_brake_count");
                    long harshAccel = rs.getLong("harsh_accel_count");
                    long speeding = rs.getLong("speeding_count");

                    // naive placeholder: weight harsh events higher than speeding,
                    // cap at 100. Real methodology is task 2.
                    int riskScore = (int) Math.min(100, (harshBrake * 6) + (harshAccel * 6) + (speeding * 3));

                    return new DashboardModels.CarRiskSummary(
                            rs.getString("car_id"),
                            rs.getLong("trip_count"),
                            rs.getDouble("avg_speed_kmh"),
                            rs.getDouble("max_speed_kmh"),
                            harshBrake,
                            harshAccel,
                            speeding,
                            riskScore
                    );
                }
        );
    }

    public List<DashboardModels.TripRow> fetchRecentTrips(int limit) {
        return jdbcTemplate.query(
                """
                SELECT trip_id, car_id, trip_start, trip_end, status, ping_count,
                       avg_speed_kmh, max_speed_kmh, harsh_brake_count, harsh_accel_count, speeding_count
                FROM car_trip_behavior
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (rs, rowNum) -> new DashboardModels.TripRow(
                        rs.getString("trip_id"),
                        rs.getString("car_id"),
                        rs.getTimestamp("trip_start"),
                        rs.getTimestamp("trip_end"),
                        rs.getString("status"),
                        rs.getLong("ping_count"),
                        rs.getDouble("avg_speed_kmh"),
                        rs.getDouble("max_speed_kmh"),
                        rs.getInt("harsh_brake_count"),
                        rs.getInt("harsh_accel_count"),
                        rs.getInt("speeding_count")
                ),
                limit
        );
    }
}
