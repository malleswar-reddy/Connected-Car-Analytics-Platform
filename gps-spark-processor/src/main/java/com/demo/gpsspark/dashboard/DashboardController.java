package com.demo.gpsspark.dashboard;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * One call, everything the dashboard needs: summary KPIs, per-car risk
 * rollup, and a recent-trips feed. Deliberately not three separate endpoints
 * — the frontend renders all three sections from a single response so there's
 * one loading/error state to manage instead of three.
 */
@RestController
// demo-only: wide open so a frontend on a different origin (e.g. a Vite dev
// server on :5173, or a claude.ai-hosted artifact) can call this directly.
// Lock this down to your real frontend origin before this goes near production.
@CrossOrigin(origins = "*")
public class DashboardController {

    private final DashboardRepository repository;

    public DashboardController(DashboardRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/api/dashboard")
    public DashboardModels.DashboardResponse dashboard(
            @RequestParam(defaultValue = "20") int recentTripsLimit) {
        return new DashboardModels.DashboardResponse(
                repository.fetchSummary(),
                repository.fetchPerCar(),
                repository.fetchRecentTrips(recentTripsLimit)
        );
    }
}
