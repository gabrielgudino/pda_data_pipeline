CREATE TABLE date_dim (
    date_id BIGINT IDENTITY(1,1), 
    date DATE NOT NULL,             -- Fecha en formato DATE
    day INT,                        -- Día del mes (1-31)
    day_of_week INT,                -- Día de la semana (1=Lunes, 7=Domingo)
    day_name VARCHAR(10),           -- Nombre del día (Lunes, Martes, etc.)
    week INT,                       -- Número de semana en el año
    month INT,                      -- Mes (1-12)
    month_name VARCHAR(15),         -- Nombre del mes (Enero, Febrero, etc.)
    quarter INT,                    -- Trimestre (1-4)
    year INT,                       -- Año (2024, 2025, etc.)
    is_weekend BOOLEAN,             -- Indica si es fin de semana (TRUE si es sábado o domingo)
    PRIMARY KEY (date_id)
);
