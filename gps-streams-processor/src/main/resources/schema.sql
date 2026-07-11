CREATE TABLE IF NOT EXISTS gps_ping (
    id              BIGSERIAL PRIMARY KEY,
    car_id          VARCHAR(64)  NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    speed_kmh       DOUBLE PRECISION NOT NULL,
    heading         DOUBLE PRECISION NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gps_ping_car_id ON gps_ping (car_id);
CREATE INDEX IF NOT EXISTS idx_gps_ping_event_ts ON gps_ping (event_timestamp);

CREATE TABLE IF NOT EXISTS car_speed_window_stats (
    car_id       VARCHAR(64) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    ping_count   BIGINT NOT NULL,
    avg_speed    DOUBLE PRECISION NOT NULL,
    max_speed    DOUBLE PRECISION NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (car_id, window_start)
);
