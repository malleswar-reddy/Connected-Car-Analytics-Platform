package com.demo.gpsstreams.web;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class GpsQueryController {

    private final JdbcTemplate jdbcTemplate;

    public GpsQueryController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/api/pings/{carId}")
    public List<Map<String, Object>> recentPings(@PathVariable String carId) {
        return jdbcTemplate.queryForList(
                "SELECT * FROM gps_ping WHERE car_id = ? ORDER BY event_timestamp DESC LIMIT 20",
                carId);
    }

    @GetMapping("/api/speed-stats/{carId}")
    public List<Map<String, Object>> speedStats(@PathVariable String carId) {
        return jdbcTemplate.queryForList(
                "SELECT * FROM car_speed_window_stats WHERE car_id = ? ORDER BY window_start DESC LIMIT 20",
                carId);
    }
}
