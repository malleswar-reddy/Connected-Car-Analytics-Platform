package com.demo.gpsspark.processing;

import com.demo.gpsspark.model.DrivingState;
import com.demo.gpsspark.model.GpsPing;
import com.demo.gpsspark.model.TripEvent;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Runs once per car per micro-batch (Spark groups all new rows for a key and
 * calls this once with all of them, in the order they arrived in the batch).
 *
 * Trip segmentation: a "trip" is a run of pings with no gap larger than
 * tripGapMillis between consecutive pings. A gap closes the current trip and
 * opens a new one. A car that simply stops sending data (parked, app closed)
 * is handled by the processing-time timeout, since there's no "next ping" to
 * detect the gap from.
 *
 * Harsh braking / harsh acceleration: computed as (speed delta) / (time delta)
 * between consecutive pings within the same trip — i.e. approximate
 * acceleration in km/h per second. Thresholds are simplifications for this
 * demo (see class javadoc in GpsDrivingBehaviorJob for caveats).
 *
 * Speeding: flat threshold against speedLimitKmh. A real system would need
 * per-road-segment limits via map-matching; this is a placeholder.
 */
public class DrivingBehaviorStateFunction
        implements FlatMapGroupsWithStateFunction<String, GpsPing, DrivingState, TripEvent> {

    private final long tripGapMillis;
    private final double harshBrakeThresholdKmhPerSec; // negative number, e.g. -10.0
    private final double harshAccelThresholdKmhPerSec; // positive number, e.g. 8.0
    private final double speedLimitKmh;

    public DrivingBehaviorStateFunction(long tripGapMillis,
                                         double harshBrakeThresholdKmhPerSec,
                                         double harshAccelThresholdKmhPerSec,
                                         double speedLimitKmh) {
        this.tripGapMillis = tripGapMillis;
        this.harshBrakeThresholdKmhPerSec = harshBrakeThresholdKmhPerSec;
        this.harshAccelThresholdKmhPerSec = harshAccelThresholdKmhPerSec;
        this.speedLimitKmh = speedLimitKmh;
    }

    @Override
    public Iterator<TripEvent> call(String carId, Iterator<GpsPing> pings, GroupState<DrivingState> state) {
        List<TripEvent> output = new ArrayList<>();

        // Car went quiet for longer than the trip gap with no new ping to tell
        // us so directly -- close whatever trip was open and drop the state.
        if (state.hasTimedOut()) {
            if (state.exists()) {
                output.add(toTripEvent(carId, state.get(), "CLOSED"));
            }
            state.remove();
            return output.iterator();
        }

        DrivingState current = state.exists() ? state.get() : null;

        while (pings.hasNext()) {
            GpsPing ping = pings.next();
            long eventMillis = ping.getTimestamp().getTime();

            boolean isNewTrip = current == null || (eventMillis - current.getLastEventEpochMillis()) > tripGapMillis;

            if (isNewTrip) {
                if (current != null) {
                    output.add(toTripEvent(carId, current, "CLOSED")); // gap detected -> close previous trip
                }
                current = new DrivingState();
                current.setTripId(carId + "-" + eventMillis);
                current.setTripStartEpochMillis(eventMillis);
                current.setLastEventEpochMillis(eventMillis);
                current.setLastSpeedKmh(ping.getSpeedKmh());
            } else {
                double deltaSeconds = (eventMillis - current.getLastEventEpochMillis()) / 1000.0;
                if (deltaSeconds > 0) {
                    double acceleration = (ping.getSpeedKmh() - current.getLastSpeedKmh()) / deltaSeconds;
                    if (acceleration <= harshBrakeThresholdKmhPerSec) {
                        current.setHarshBrakeCount(current.getHarshBrakeCount() + 1);
                    } else if (acceleration >= harshAccelThresholdKmhPerSec) {
                        current.setHarshAccelCount(current.getHarshAccelCount() + 1);
                    }
                }
                current.setLastEventEpochMillis(eventMillis);
                current.setLastSpeedKmh(ping.getSpeedKmh());
            }

            if (ping.getSpeedKmh() > speedLimitKmh) {
                current.setSpeedingCount(current.getSpeedingCount() + 1);
            }

            current.setPingCount(current.getPingCount() + 1);
            current.setSumSpeedKmh(current.getSumSpeedKmh() + ping.getSpeedKmh());
            current.setMaxSpeedKmh(Math.max(current.getMaxSpeedKmh(), ping.getSpeedKmh()));
        }

        state.update(current);
        // must be re-set every call under ProcessingTimeTimeout, or the previous
        // timeout keeps counting down from the last time it was set
        state.setTimeoutDuration(tripGapMillis);

        output.add(toTripEvent(carId, current, "OPEN")); // progress snapshot for this micro-batch

        return output.iterator();
    }

    private TripEvent toTripEvent(String carId, DrivingState s, String status) {
        double avgSpeed = s.getPingCount() == 0 ? 0.0 : s.getSumSpeedKmh() / s.getPingCount();
        return new TripEvent(
                s.getTripId(),
                carId,
                new Timestamp(s.getTripStartEpochMillis()),
                new Timestamp(s.getLastEventEpochMillis()),
                status,
                s.getPingCount(),
                avgSpeed,
                s.getMaxSpeedKmh(),
                s.getHarshBrakeCount(),
                s.getHarshAccelCount(),
                s.getSpeedingCount()
        );
    }
}
