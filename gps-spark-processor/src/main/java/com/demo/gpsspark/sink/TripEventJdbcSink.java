package com.demo.gpsspark.sink;

import com.demo.gpsspark.model.TripEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Iterator;

/**
 * Upserts TripEvent rows into car_trip_behavior. Runs on the executor (called
 * from Dataset.foreachPartition inside a foreachBatch sink), so each
 * partition opens and closes its own JDBC connection -- standard pattern for
 * Spark's foreachBatch + JDBC, avoids funneling everything through the driver.
 * Implements AutoCloseable to support memory-safe try-with-resources cleanups.
 */
public class TripEventJdbcSink implements AutoCloseable {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public TripEventJdbcSink(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    public void writePartition(Iterator<TripEvent> rows) {
        String sql = """
                INSERT INTO car_trip_behavior
                    (trip_id, car_id, trip_start, trip_end, status, ping_count,
                     avg_speed_kmh, max_speed_kmh, harsh_brake_count, harsh_accel_count,
                     speeding_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (trip_id) DO UPDATE SET
                    trip_end          = EXCLUDED.trip_end,
                    status            = EXCLUDED.status,
                    ping_count        = EXCLUDED.ping_count,
                    avg_speed_kmh     = EXCLUDED.avg_speed_kmh,
                    max_speed_kmh     = EXCLUDED.max_speed_kmh,
                    harsh_brake_count = EXCLUDED.harsh_brake_count,
                    harsh_accel_count = EXCLUDED.harsh_accel_count,
                    speeding_count    = EXCLUDED.speeding_count,
                    updated_at        = EXCLUDED.updated_at
                """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            int batched = 0;

            while (rows.hasNext()) {
                TripEvent e = rows.next();
                ps.setString(1, e.getTripId());
                ps.setString(2, e.getCarId());
                ps.setTimestamp(3, e.getTripStart());
                ps.setTimestamp(4, e.getTripEnd());
                ps.setString(5, e.getStatus());
                ps.setLong(6, e.getPingCount());
                ps.setDouble(7, e.getAvgSpeedKmh());
                ps.setDouble(8, e.getMaxSpeedKmh());
                ps.setInt(9, e.getHarshBrakeCount());
                ps.setInt(10, e.getHarshAccelCount());
                ps.setInt(11, e.getSpeedingCount());
                ps.setTimestamp(12, new Timestamp(System.currentTimeMillis()));
                ps.addBatch();

                batched++;
                if (batched % 500 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            conn.commit();

        } catch (Exception e) {
            throw new RuntimeException("Failed to write trip events batch to Postgres", e);
        }
    }

    @Override
    public void close() throws Exception {
        // SQL resources are scoped tightly inside the writePartition try-with-resources.
        // This method satisfies the AutoCloseable interface requirements.
    }
}