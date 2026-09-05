package com.demo.gpssimulator.web;

import com.demo.gpssimulator.dto.ScenarioResponse;
import com.demo.gpssimulator.scheduler.GpsSimulatorScheduler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Lets you deliberately provoke the behavior the downstream Spark job is
 * looking for, on demand, instead of waiting for it to happen (or not) by
 * chance -- useful for demoing/testing the driving-behavior dashboard.
 *
 * carId is optional on both: omit it (or pass one that doesn't exist) and a
 * random currently-simulated car is used instead.
 */
@RestController
// demo-only: the dashboard lives on :8083, this app on :8081 -- lock this
// down to the real frontend origin before this goes near production.
@CrossOrigin(origins = "*")
public class ScenarioController {

    private final GpsSimulatorScheduler scheduler;

    public ScenarioController(GpsSimulatorScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostMapping("/api/simulator/scenarios/harsh-driving")
    public ResponseEntity<ScenarioResponse> harshDriving(
            @RequestParam(required = false) String carId) {
        var result = scheduler.triggerHarshDriving(carId);
        return ResponseEntity.ok(new ScenarioResponse(result.carId(), "HARSH_DRIVING", result.queuedSpeedsKmh()));
    }

    @PostMapping("/api/simulator/scenarios/speeding")
    public ResponseEntity<ScenarioResponse> speeding(
            @RequestParam(required = false) String carId,
            @RequestParam(required = false) Integer durationTicks) {
        var result = scheduler.triggerSpeeding(carId, durationTicks);
        return ResponseEntity.ok(new ScenarioResponse(result.carId(), "SPEEDING", result.queuedSpeedsKmh()));
    }
}
