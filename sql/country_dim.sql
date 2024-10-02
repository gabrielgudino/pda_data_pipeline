CREATE TABLE country_dim (
    country_id BIGINT IDENTITY(1,1),
    country_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (country_id)
);
