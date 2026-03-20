import requests
import psycopg2
import pandas as pd
from datetime import datetime

URL = "https://api.coingecko.com/api/v3/simple/price"

PARAMS = {
    "ids": "bitcoin,ethereum",
    "vs_currencies": "usd"
}

def extract():
    response = requests.get(URL, params=PARAMS)
    return response.json()

def transform(data):
    df = pd.DataFrame(data).T.reset_index()
    df.columns = ["coin", "price_usd"]

    df["extracted_at"] = pd.Timestamp.now('UTC')

    df["price_usd"] = df["price_usd"].astype(float)

    return df

def load_raw(df):
    conn = psycopg2.connect(
        host="localhost",
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
            INSERT INTO raw_prices (coin, price_usd, extracted_at)
            VALUES (%s, %s, %s)
            """,
            (row["coin"], row["price_usd"], row["extracted_at"])
        )

    conn.commit()
    cursor.close()
    conn.close()

def transform_sql():
    conn = psycopg2.connect(
        host="localhost",
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
        INSERT INTO clean_prices (coin, price_usd, extracted_at)
        SELECT coin, price_usd, extracted_at
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