package com.demo.gpssimulator.scheduler;

import com.demo.gpssimulator.model.GpsData;
import com.demo.gpssimulator.producer.GpsDataProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulates N cars roaming around Bengaluru, India and publishes a GPS ping
 * for each car on a fixed interval.
 */
@Component
public class GpsSimulatorScheduler {

    // Updated coordinates to target Bengaluru, Karnataka, India
    private static final double BASE_LAT = 12.9716;
    private static final double BASE_LON = 77.5946;
    private final GpsDataProducer producer;
    private final int carCount;
    private final Map<String, double[]> carState = new HashMap<>(); // carId -> [lat, lon, heading]

    public GpsSimulatorScheduler(GpsDataProducer producer,
                                 @Value("${gps.car-count}") int carCount) {
        this.producer = producer;
        this.carCount = carCount;
        initCars();
    }

    private void initCars() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 1; i <= carCount; i++) {
            String carId = "car-" + i;
            double lat = BASE_LAT + random.nextDouble(-0.05, 0.05);
            double lon = BASE_LON + random.nextDouble(-0.05, 0.05);
            double heading = random.nextDouble(0, 360);
            carState.put(carId, new double[]{lat, lon, heading});
        }
    }

    @Scheduled(fixedRateString = "${gps.publish-interval-ms}")
    public void publishTick() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        carState.forEach((carId, state) -> {
            double heading = (state[2] + random.nextDouble(-15, 15) + 360) % 360;
            double speedKmh = random.nextDouble(0, 120);

            double distanceDegrees = (speedKmh / 3600.0) * (2000.0 / 111_000.0);
            double headingRad = Math.toRadians(heading);
            double newLat = state[0] + distanceDegrees * Math.cos(headingRad);
            double newLon = state[1] + distanceDegrees * Math.sin(headingRad);

            state[0] = newLat;
            state[1] = newLon;
            state[2] = heading;

            GpsData ping = new GpsData(carId, newLat, newLon, speedKmh, heading, Instant.now());
            producer.send(ping);
        });
    }
}