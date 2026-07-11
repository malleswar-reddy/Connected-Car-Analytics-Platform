package com.demo.gpsspark.web;

import com.demo.gpsspark.service.DrivingBehaviorStreamingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/spark/driving-behavior")
public class SparkJobController {

    private final DrivingBehaviorStreamingService service;

    public SparkJobController(DrivingBehaviorStreamingService service) {
        this.service = service;
    }

    /** Idempotent: calling start on an already-running job just returns its current status. */
    @PostMapping("/start")
    public ResponseEntity<JobStatus> start() {
        try {
            return ResponseEntity.ok(service.start());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new JobStatus(false, null, null, null, e.getMessage()));
        }
    }

    /** Idempotent: calling stop when nothing is running just returns the current (not-running) status. */
    @PostMapping("/stop")
    public ResponseEntity<JobStatus> stop() {
        try {
            return ResponseEntity.ok(service.stop());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new JobStatus(false, null, null, null, e.getMessage()));
        }
    }

    @GetMapping("/status")
    public ResponseEntity<JobStatus> status() {
        return ResponseEntity.ok(service.status());
    }
}
