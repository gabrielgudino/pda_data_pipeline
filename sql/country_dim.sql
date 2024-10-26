CREATE TABLE IF NOT EXISTS country_dim (
    country_id BIGINT IDENTITY(1,1),
    country_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (country_id)
);
