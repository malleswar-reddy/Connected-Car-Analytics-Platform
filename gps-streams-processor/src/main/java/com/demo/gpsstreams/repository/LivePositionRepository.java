package com.demo.gpsstreams.repository;

import com.demo.gpsstreams.model.CarPosition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class LivePositionRepository {

    private final JdbcTemplate jdbcTemplate;

    public LivePositionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * One row per car: whichever ping has the latest event_timestamp.
     * Backed by idx_gps_ping_car_id_event_ts (car_id, event_timestamp DESC),
     * so this stays cheap even as gps_ping grows.
     */
    public List<CarPosition> findLatestPerCar() {
        return jdbcTemplate.query(
                """
                        SELECT DISTINCT ON (car_id)
                            car_id, latitude, longitude, speed_kmh, heading, event_timestamp
                        FROM gps_ping
                        ORDER BY car_id, event_timestamp DESC
                        """,
                (rs, rowNum) -> new CarPosition(
                        rs.getString("car_id"),
                        rs.getDouble("latitude"),
                        rs.getDouble("longitude"),
                        rs.getDouble("speed_kmh"),
                        rs.getDouble("heading"),
                        rs.getTimestamp("event_timestamp").toInstant()
                )
        );
    }
}
