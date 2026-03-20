CREATE TABLE raw_prices (
    id SERIAL PRIMARY KEY,
    coin VARCHAR(50),
    price_usd FLOAT,
    extracted_at TIMESTAMP
)

CREATE TABLE clean_prices (
    id SERIAL PRIMARY KEY,
    coin VARCHAR(50),
    price_usd FLOAT,
    extracted_at TIMESTAMP
)