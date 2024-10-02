CREATE TABLE region_dim (
    region_id BIGINT IDENTITY(1,1), 
    region_name VARCHAR(255),
    country_id BIGINT NOT NULL, 
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (region_id),
    FOREIGN KEY (country_id) REFERENCES country_dim(country_id)
);
