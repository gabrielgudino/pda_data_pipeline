CREATE TABLE time_dim (
    time_id BIGINT IDENTITY(1,1),
    localtime_epoch BIGINT,
    local_time TIMESTAMP,
    last_updated_epoch BIGINT,
    last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (time_id)
);
