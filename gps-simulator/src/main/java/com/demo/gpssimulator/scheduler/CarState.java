package com.demo.gpssimulator.scheduler;

/**
 * Mutable per-car simulation state. Speed is carried across ticks (smoothed
 * random walk) rather than redrawn from scratch each time, so normal driving
 * stays plausible and a deliberately-triggered harsh-driving/speeding
 * scenario actually stands out against it instead of blending into constant
 * background noise.
 */
class CarState {
    double latitude;
    double longitude;
    double heading;
    double speedKmh;

    CarState(double latitude, double longitude, double heading, double speedKmh) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.heading = heading;
        this.speedKmh = speedKmh;
    }
}
