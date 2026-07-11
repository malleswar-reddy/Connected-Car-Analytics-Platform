package com.demo.gpsstreams.repository;

import com.demo.gpsstreams.model.SpeedStats;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;

@Repository
public class SpeedStatsRepository {

    private final JdbcTemplate jdbcTemplate;

    public SpeedStatsRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void upsert(SpeedStats stats) {
        // Kafka Streams re-emits an updated aggregate on every new record in an
        // open window, so this is called repeatedly for the same (car_id, window_start) —
        // hence upsert rather than insert.
        jdbcTemplate.update(
                """
                        INSERT INTO car_speed_window_stats
                            (car_id, window_start, window_end, ping_count, avg_speed, max_speed, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (car_id, window_start)
                        DO UPDATE SET
                            window_end = EXCLUDED.window_end,
                            ping_count = EXCLUDED.ping_count,
                            avg_speed  = EXCLUDED.avg_speed,
                            max_speed  = EXCLUDED.max_speed,
                            updated_at = EXCLUDED.updated_at
                        """,
                stats.getCarId(),
                Timestamp.from(stats.getWindowStart()),
                Timestamp.from(stats.getWindowEnd()),
                stats.getPingCount(),
                stats.getAvgSpeed(),
                stats.getMaxSpeed(),
                Timestamp.from(Instant.now())
        );
    }
}
