package com.demo.gpsspark.service;

import com.demo.gpsspark.model.DrivingState;
import com.demo.gpsspark.model.GpsPing;
import com.demo.gpsspark.model.TripEvent;
import com.demo.gpsspark.processing.DrivingBehaviorStateFunction;
import com.demo.gpsspark.sink.TripEventJdbcSink;
import com.demo.gpsspark.web.JobStatus;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.to_timestamp;

/**
 * Owns the embedded SparkSession and the driving-behavior StreamingQuery.
 * start()/stop() are non-blocking: writeStream().start() returns a
 * StreamingQuery handle immediately and Spark keeps running the micro-batches
 * on its own threads, so a REST call can trigger this and return right away
 * instead of blocking the HTTP thread on awaitTermination().
 *
 * See GpsDrivingBehaviorJob's original javadoc-equivalent caveats, still true
 * here: fixed harsh-brake/accel thresholds, flat speed limit (no map
 * matching), ProcessingTimeTimeout (wall-clock) rather than event-time
 * timeout for closing trips on inactivity.
 */
@Slf4j
@Service
public class DrivingBehaviorStreamingService {

    private final String bootstrapServers;
    private final String inputTopic;
    private final String checkpointLocation;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final String sparkMaster;
    private final long tripGapMillis;
    private final double harshBrakeThresholdKmhPerSec;
    private final double harshAccelThresholdKmhPerSec;
    private final double speedLimitKmh;

    private SparkSession sparkSession;
    private StreamingQuery streamingQuery;

    public DrivingBehaviorStreamingService(
            @Value("${gps.bootstrap-servers}") String bootstrapServers,
            @Value("${gps.input-topic}") String inputTopic,
            @Value("${gps.checkpoint-location}") String checkpointLocation,
            @Value("${gps.jdbc-url}") String jdbcUrl,
            @Value("${gps.jdbc-user}") String jdbcUser,
            @Value("${gps.jdbc-password}") String jdbcPassword,
            @Value("${gps.spark-master}") String sparkMaster,
            @Value("${gps.trip-gap-minutes}") long tripGapMinutes,
            @Value("${gps.harsh-brake-threshold}") double harshBrakeThreshold,
            @Value("${gps.harsh-accel-threshold}") double harshAccelThresholdKmhPerSec,
            @Value("${gps.speed-limit-kmh}") double speedLimitKmh) {
        this.bootstrapServers = bootstrapServers;
        this.inputTopic = inputTopic;
        this.checkpointLocation = checkpointLocation;
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.sparkMaster = sparkMaster;
        this.tripGapMillis = tripGapMinutes * 60_000L;
        this.harshBrakeThresholdKmhPerSec = -harshBrakeThreshold; // stored/compared as negative
        this.harshAccelThresholdKmhPerSec = harshAccelThresholdKmhPerSec;
        this.speedLimitKmh = speedLimitKmh;
    }

    public synchronized JobStatus start() {
        if (streamingQuery != null && streamingQuery.isActive()) {
            log.info("Start requested but job is already running (queryId={})", streamingQuery.id());
            return status();
        }

        try {
            ensureTableExists();

            if (sparkSession == null) {
                sparkSession = SparkSession.builder()
                        .appName("gps-driving-behavior-job")
                        .master(sparkMaster)
                        .config("spark.ui.enabled", "false")

                        // Memory Management Tuning for Embedded Application Services
                        .config("spark.sql.streaming.minBatchesToRetain", "2")
                        .config("spark.sql.ui.retainedExecutionPerQuery", "5")
                        .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
                        .getOrCreate();
                // sparkSession.sparkContext().setLogLevel("WARN");
            }

            StructType gpsSchema = new StructType()
                    .add("carId", DataTypes.StringType)
                    .add("latitude", DataTypes.DoubleType)
                    .add("longitude", DataTypes.DoubleType)
                    .add("speedKmh", DataTypes.DoubleType)
                    .add("heading", DataTypes.DoubleType)
                    .add("timestamp", DataTypes.StringType);

            Dataset<Row> kafkaRaw = sparkSession.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", bootstrapServers)
                    .option("subscribe", inputTopic)
                    .option("startingOffsets", "latest")
                    .option("failOnDataLoss", "false") // Bypasses micro-batch crashes when offsets reset
                    .load();

            Dataset<Row> parsed = kafkaRaw
                    .selectExpr("CAST(value AS STRING) AS json")
                    .select(from_json(col("json"), gpsSchema).alias("data"))
                    .select("data.*")
                    .withColumn("timestamp", to_timestamp(col("timestamp")));

            Dataset<Row> filtered = parsed
                    .filter(col("carId").isNotNull())
                    .filter(col("timestamp").isNotNull())
                    .filter(col("speedKmh").geq(0).and(col("speedKmh").leq(400)))
                    .filter(col("latitude").between(-90, 90))
                    .filter(col("longitude").between(-180, 180));

            Dataset<GpsPing> pings = filtered.as(Encoders.bean(GpsPing.class));

            DrivingBehaviorStateFunction stateFunction = new DrivingBehaviorStateFunction(
                    tripGapMillis, harshBrakeThresholdKmhPerSec, harshAccelThresholdKmhPerSec, speedLimitKmh);

            Dataset<TripEvent> tripEvents = pings
                    .groupByKey((org.apache.spark.api.java.function.MapFunction<GpsPing, String>) GpsPing::getCarId, Encoders.STRING())
                    .flatMapGroupsWithState(
                            stateFunction,
                            OutputMode.Append(),
                            Encoders.bean(DrivingState.class),
                            Encoders.bean(TripEvent.class),
                            GroupStateTimeout.ProcessingTimeTimeout()
                    );

            //TripEventJdbcSink sink = new TripEventJdbcSink(jdbcUrl, jdbcUser, jdbcPassword);

            streamingQuery = tripEvents.writeStream()
                    .outputMode(OutputMode.Append())
                    .foreachBatch((VoidFunction2<Dataset<TripEvent>, Long>) (batchDf, batchId) -> {
                        // Instantiate/reference parameters safely inside the lambda closure
                        final String url = jdbcUrl;
                        final String user = jdbcUser;
                        final String pass = jdbcPassword;

                        batchDf.foreachPartition((ForeachPartitionFunction<TripEvent>) partitionIterator -> {
                            // try-with-resources cleans up JDBC connections per partition task, preventing leaks
                            try (TripEventJdbcSink localSink = new TripEventJdbcSink(url, user, pass)) {
                                localSink.writePartition(partitionIterator);
                            } catch (Exception e) {
                                log.error("Error writing batch partition processing metrics to database target", e);
                                throw e;
                            }
                        });
                    })
                    .option("checkpointLocation", checkpointLocation)
                    .trigger(Trigger.ProcessingTime("10 seconds"))
                    .start();

            log.info("Driving-behavior streaming job started, queryId={}", streamingQuery.id());
            return status();

        } catch (Exception e) {
            log.error("Failed to start driving-behavior streaming job", e);
            throw new IllegalStateException("Failed to start streaming job: " + e.getMessage(), e);
        }
    }

    public synchronized JobStatus stop() {
        if (streamingQuery == null || !streamingQuery.isActive()) {
            log.info("Stop requested but no job is currently running");
            return status();
        }
        try {
            streamingQuery.stop();
            log.info("Driving-behavior streaming job stopped");
        } catch (Exception e) {
            log.error("Error while stopping streaming job", e);
            throw new IllegalStateException("Failed to stop streaming job: " + e.getMessage(), e);
        }
        return status();
    }

    public synchronized JobStatus status() {
        if (streamingQuery == null) {
            return new JobStatus(false, null, null, null, null);
        }

        boolean running = streamingQuery.isActive();
        String queryId = streamingQuery.id().toString();
        String runId = streamingQuery.runId().toString();

        StreamingQueryProgress progress = streamingQuery.lastProgress();
        String progressJson = progress != null ? progress.json() : null;

        String errorMessage = null;
        if (!running && streamingQuery.exception().isDefined()) {
            errorMessage = streamingQuery.exception().get().toString();
        }

        return new JobStatus(running, queryId, runId, progressJson, errorMessage);
    }

    private void ensureTableExists() throws Exception {
        String ddl = """
                CREATE TABLE IF NOT EXISTS car_trip_behavior (
                    trip_id            VARCHAR(128) PRIMARY KEY,
                    car_id             VARCHAR(64)  NOT NULL,
                    trip_start         TIMESTAMPTZ  NOT NULL,
                    trip_end           TIMESTAMPTZ  NOT NULL,
                    status             VARCHAR(10)  NOT NULL,
                    ping_count         BIGINT       NOT NULL,
                    avg_speed_kmh      DOUBLE PRECISION NOT NULL,
                    max_speed_kmh      DOUBLE PRECISION NOT NULL,
                    harsh_brake_count  INT NOT NULL,
                    harsh_accel_count  INT NOT NULL,
                    speeding_count     INT NOT NULL,
                    updated_at         TIMESTAMPTZ NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_car_trip_behavior_car_id ON car_trip_behavior (car_id);
                """;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
             Statement stmt = conn.createStatement()) {
            for (String statement : ddl.split(";")) {
                if (!statement.isBlank()) {
                    stmt.execute(statement);
                }
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        if (streamingQuery != null && streamingQuery.isActive()) {
            log.info("Application shutting down, stopping streaming job");
            try {
                streamingQuery.stop();
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }
}