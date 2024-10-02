CREATE TABLE location_dim (
    location_id BIGINT IDENTITY(1,1), 
    location_name VARCHAR(255) NOT NULL,
    region_id BIGINT NOT NULL, 
    lat DECIMAL(9,6), 
    lon DECIMAL(9,6), 
    tz_id VARCHAR(255), 
    PRIMARY KEY (location_id),
    FOREIGN KEY (region_id) REFERENCES region_dim(region_id)
);
