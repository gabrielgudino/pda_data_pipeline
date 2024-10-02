CREATE TABLE condition_dim (
    condition_id BIGINT IDENTITY(1,1), 
    condition_text VARCHAR(255),
    condition_icon VARCHAR(255),
    condition_code INT,
    PRIMARY KEY (condition_id)
);
