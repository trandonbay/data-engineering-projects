CREATE TABLE raw_prices (
    id SERIAL PRIMARY KEY,
    coin VARCHAR(50),
    price_usd FLOAT,
    market_cap BIGINT,
    volume BIGINT,
    rank SMALLINT,
    extracted_at TIMESTAMP
);

CREATE TABLE clean_prices (
    id SERIAL PRIMARY KEY,
    coin VARCHAR(50),
    price_usd FLOAT,
    market_cap BIGINT,
    volume BIGINT,
    rank SMALLINT,
    extracted_at TIMESTAMP
);