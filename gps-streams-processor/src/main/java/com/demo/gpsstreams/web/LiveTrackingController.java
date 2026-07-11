package com.demo.gpsstreams.web;

import com.demo.gpsstreams.model.CarPosition;
import com.demo.gpsstreams.repository.LivePositionRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * One-shot snapshot of every car's latest known position — used by the
 * frontend to render the initial map/table state before the WebSocket
 * feed at /ws/car-positions takes over for live updates.
 */
@RestController
public class LiveTrackingController {

    private final LivePositionRepository repository;

    public LiveTrackingController(LivePositionRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/api/live/cars")
    public List<CarPosition> allLatestPositions() {
        return repository.findLatestPerCar();
    }
}
