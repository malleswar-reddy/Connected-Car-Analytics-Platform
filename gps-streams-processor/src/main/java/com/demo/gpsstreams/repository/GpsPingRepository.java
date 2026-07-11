package com.demo.gpsstreams.repository;

import com.demo.gpsstreams.model.GpsData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;

@Repository
public class GpsPingRepository {

    private final JdbcTemplate jdbcTemplate;

    public GpsPingRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(GpsData ping) {
        jdbcTemplate.update(
                """
                        INSERT INTO gps_ping
                            (car_id, latitude, longitude, speed_kmh, heading, event_timestamp, received_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                ping.getCarId(),
                ping.getLatitude(),
                ping.getLongitude(),
                ping.getSpeedKmh(),
                ping.getHeading(),
                Timestamp.from(ping.getTimestamp()),
                Timestamp.from(Instant.now())
        );
    }
}
