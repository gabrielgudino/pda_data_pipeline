CREATE TABLE weather_fact (
    weather_id BIGINT IDENTITY(1,1), 
    location_id BIGINT NOT NULL,              
    condition_id BIGINT NOT NULL,    
    date_id BIGINT NOT NULL,         
    temp_c DECIMAL(5,2),
    temp_f DECIMAL(5,2),
    is_day BOOLEAN,
    wind_mph DECIMAL(5,2),
    wind_kph DECIMAL(5,2),
    wind_degree INT,
    wind_dir VARCHAR(10),
    pressure_mb DECIMAL(6,2),
    pressure_in DECIMAL(6,2),
    precip_mm DECIMAL(6,2),
    precip_in DECIMAL(6,2),
    humidity INT,
    cloud INT,
    feelslike_c DECIMAL(5,2),
    feelslike_f DECIMAL(5,2),
    windchill_c DECIMAL(5,2),
    windchill_f DECIMAL(5,2),
    heatindex_c DECIMAL(5,2),
    heatindex_f DECIMAL(5,2),
    dewpoint_c DECIMAL(5,2),
    dewpoint_f DECIMAL(5,2),
    vis_km DECIMAL(5,2),
    vis_miles DECIMAL(5,2),
    uv DECIMAL(5,2),
    gust_mph DECIMAL(5,2),
    gust_kph DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (weather_id),
    FOREIGN KEY (location_id) REFERENCES location_dim(location_id),
    FOREIGN KEY (condition_id) REFERENCES condition_dim(condition_id),
    FOREIGN KEY (date_id) REFERENCES date_dim(date_id) 
);
