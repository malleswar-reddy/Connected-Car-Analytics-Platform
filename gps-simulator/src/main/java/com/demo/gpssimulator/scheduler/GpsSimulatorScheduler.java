package com.demo.gpssimulator.scheduler;

import com.demo.gpssimulator.model.GpsData;
import com.demo.gpssimulator.producer.GpsDataProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulates N cars roaming around Bengaluru, India and publishes a GPS ping
 * for each car on a fixed interval.
 *
 * Normal driving: speed does a bounded random walk each tick (±maxSpeedChange
 * per tick) rather than being redrawn from scratch, so consecutive pings stay
 * physically plausible and don't accidentally look like harsh braking.
 *
 * Scenario injection: {@link #triggerHarshDriving(String)} and
 * {@link #triggerSpeeding(String, Integer)} push forced speed values into a
 * per-car override queue; publishTick() drains that queue before falling
 * back to the normal random walk, so a triggered scenario overrides ordinary
 * driving for exactly as many ticks as it queued.
 */
@Component
public class GpsSimulatorScheduler {

    // Updated coordinates to target Bengaluru, Karnataka, India
    private static final double BASE_LAT = 12.9716;
    private static final double BASE_LON = 77.5946;
    private final GpsDataProducer producer;
    private final int carCount;

    private final double normalMaxSpeedKmh;
    private final double normalMaxSpeedChangeKmh;
    private final double harshAccelSpikeKmh;
    private final double harshBrakeDropKmh;
    private final double speedingMinOverKmh;
    private final double speedingMaxOverKmh;
    private final int speedingDefaultDurationTicks;
    private final double speedLimitKmh;

    private final Map<String, CarState> carState = new LinkedHashMap<>();
    private final Map<String, Queue<Double>> overrideQueues = new ConcurrentHashMap<>();

    public GpsSimulatorScheduler(
            GpsDataProducer producer,
            @Value("${gps.car-count}") int carCount,
            @Value("${gps.normal-driving.max-speed-kmh}") double normalMaxSpeedKmh,
            @Value("${gps.normal-driving.max-speed-change-per-tick-kmh}") double normalMaxSpeedChangeKmh,
            @Value("${gps.harsh-driving-scenario.accel-spike-kmh}") double harshAccelSpikeKmh,
            @Value("${gps.harsh-driving-scenario.brake-drop-kmh}") double harshBrakeDropKmh,
            @Value("${gps.speeding-scenario.min-over-kmh}") double speedingMinOverKmh,
            @Value("${gps.speeding-scenario.max-over-kmh}") double speedingMaxOverKmh,
            @Value("${gps.speeding-scenario.default-duration-ticks}") int speedingDefaultDurationTicks,
            @Value("${gps.speed-limit-kmh}") double speedLimitKmh) {
        this.producer = producer;
        this.carCount = carCount;
        this.normalMaxSpeedKmh = normalMaxSpeedKmh;
        this.normalMaxSpeedChangeKmh = normalMaxSpeedChangeKmh;
        this.harshAccelSpikeKmh = harshAccelSpikeKmh;
        this.harshBrakeDropKmh = harshBrakeDropKmh;
        this.speedingMinOverKmh = speedingMinOverKmh;
        this.speedingMaxOverKmh = speedingMaxOverKmh;
        this.speedingDefaultDurationTicks = speedingDefaultDurationTicks;
        this.speedLimitKmh = speedLimitKmh;
        initCars();
    }

    private void initCars() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 1; i <= carCount; i++) {
            String carId = "car-" + i;
            double lat = BASE_LAT + random.nextDouble(-0.05, 0.05);
            double lon = BASE_LON + random.nextDouble(-0.05, 0.05);
            double heading = random.nextDouble(0, 360);
            double startSpeed = random.nextDouble(20, 60);
            carState.put(carId, new CarState(lat, lon, heading, startSpeed));
        }
    }

    @Scheduled(fixedRateString = "${gps.publish-interval-ms}")
    public void publishTick() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        carState.forEach((carId, state) -> {
            double heading = (state.heading + random.nextDouble(-15, 15) + 360) % 360;

            Double forcedSpeed = pollOverride(carId);
            double speedKmh = forcedSpeed != null
                    ? forcedSpeed
                    : clamp(state.speedKmh + random.nextDouble(-normalMaxSpeedChangeKmh, normalMaxSpeedChangeKmh),
                            0, normalMaxSpeedKmh);

            double distanceDegrees = (speedKmh / 3600.0) * (2000.0 / 111_000.0);
            double headingRad = Math.toRadians(heading);
            double newLat = state.latitude + distanceDegrees * Math.cos(headingRad);
            double newLon = state.longitude + distanceDegrees * Math.sin(headingRad);

            state.latitude = newLat;
            state.longitude = newLon;
            state.heading = heading;
            state.speedKmh = speedKmh;

            GpsData ping = new GpsData(carId, newLat, newLon, speedKmh, heading, Instant.now());
            producer.send(ping);
        });
    }

    /**
     * Queues a sharp acceleration spike immediately followed by a sharp
     * braking drop for the given car (random car if carId is null/unknown),
     * comfortably past typical harsh-brake/accel thresholds over one publish
     * interval so it's unambiguous in downstream processing.
     */
    public TriggerResult triggerHarshDriving(String carId) {
        String resolvedCarId = resolveCarId(carId);
        CarState state = carState.get(resolvedCarId);

        double spikeSpeed = clamp(state.speedKmh + harshAccelSpikeKmh, 0, normalMaxSpeedKmh + harshAccelSpikeKmh);
        double dropSpeed = clamp(spikeSpeed - harshBrakeDropKmh, 0, normalMaxSpeedKmh + harshAccelSpikeKmh);

        Queue<Double> queue = overrideQueues.computeIfAbsent(resolvedCarId, k -> new ConcurrentLinkedQueue<>());
        queue.add(spikeSpeed);
        queue.add(dropSpeed);

        return new TriggerResult(resolvedCarId, List.of(spikeSpeed, dropSpeed));
    }

    /**
     * Queues durationTicks pings above speedLimitKmh for the given car
     * (random car if carId is null/unknown).
     */
    public TriggerResult triggerSpeeding(String carId, Integer durationTicks) {
        String resolvedCarId = resolveCarId(carId);
        int ticks = (durationTicks == null || durationTicks < 1) ? speedingDefaultDurationTicks : durationTicks;

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Queue<Double> queue = overrideQueues.computeIfAbsent(resolvedCarId, k -> new ConcurrentLinkedQueue<>());

        List<Double> queued = new ArrayList<>();
        for (int i = 0; i < ticks; i++) {
            double speed = speedLimitKmh + random.nextDouble(speedingMinOverKmh, speedingMaxOverKmh);
            queue.add(speed);
            queued.add(speed);
        }

        return new TriggerResult(resolvedCarId, queued);
    }

    public boolean carExists(String carId) {
        return carId != null && carState.containsKey(carId);
    }

    private String resolveCarId(String carId) {
        if (carId != null && carState.containsKey(carId)) {
            return carId;
        }
        List<String> carIds = new ArrayList<>(carState.keySet());
        return carIds.get(ThreadLocalRandom.current().nextInt(carIds.size()));
    }

    private Double pollOverride(String carId) {
        Queue<Double> queue = overrideQueues.get(carId);
        if (queue == null) {
            return null;
        }
        try {
            return queue.remove();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    private static double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    public record TriggerResult(String carId, List<Double> queuedSpeedsKmh) {
    }
}
