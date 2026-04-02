import requests
import psycopg2
import pandas as pd
from datetime import datetime
import os

# API endpoint to retrieve top N coins
URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
    "precision": "2",
}
HEADERS = { "x-cg-demo-api-key": os.getenv("COINGECKO_API_KEY") }

def extract():
    response = requests.get(URL, params=PARAMS, headers=HEADERS)
    return response.json()

def transform(data):
    df = pd.DataFrame(data)
    df = df[["name", "current_price", "market_cap", "total_volume", "market_cap_rank"]]
    df.columns = ["coin", "price_usd", "market_cap", "volume", "rank"]
    df["extracted_at"] = pd.Timestamp.now('UTC')
    df["price_usd"] = df["price_usd"].astype(float)

    return df

def load_raw(df):
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="crypto",
        user="admin",
        password="admin"
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM raw_prices
        """
    )

    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO raw_prices (coin, price_usd, market_cap, volume, rank, extracted_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (row["coin"], row["price_usd"], row["market_cap"], row["volume"], row["rank"], row["extracted_at"])
        )

    conn.commit()
    cursor.close()
    conn.close()

def transform_sql():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="crypto",
        user="admin",
        password="admin"
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM clean_prices
        """
    )

    cursor.execute(
        """
        INSERT INTO clean_prices (coin, price_usd, market_cap, volume, rank, extracted_at)
        SELECT coin, price_usd, market_cap, volume, rank, extracted_at
        FROM raw_prices
        WHERE price_usd IS NOT NULL
        """
    )

    conn.commit()
    cursor.close()
    conn.close()

def main():
    data = extract()
    df = transform(data)
    load_raw(df)
    transform_sql()

    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()