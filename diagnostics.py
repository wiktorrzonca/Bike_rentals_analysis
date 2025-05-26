import mysql.connector
import json
import pandas as pd

def load_config(path="DB_CONFIG.json"):
    with open(path, "r") as f:
        return json.load(f)

def run_diagnostics():
    config = load_config()
    conn = mysql.connector.connect(**config)
    diagnostics = {}

    def query(sql):
        return pd.read_sql(sql, conn)

    # === ANALIZA MIESIĘCY LETNICH I ZIMOWYCH ===
    summer_months = (7, 8, 9)
    winter_months = (1, 2, 3)

    seasonal_stats = {}

    for season, months in [("summer", summer_months), ("winter", winter_months)]:
        df = query(f"""
            SELECT 
                DATE(start_time) as ride_date,
                duration_seconds
            FROM trip_history
            WHERE MONTH(start_time) IN {months}
        """)
        total_rides = len(df)
        avg_daily_rides = df.groupby("ride_date").size().mean()
        avg_duration = df["duration_seconds"].mean()

        seasonal_stats[season] = {
            "total_rides": int(total_rides),
            "average_daily_rides": round(avg_daily_rides, 2),
            "average_duration_seconds": round(avg_duration, 2)
        }

    diagnostics["seasonal_comparison"] = seasonal_stats

    # === ZAPIS DO PLIKU JSON ===
    with open("data/diagnostic_report.json", "w") as f:
        json.dump(diagnostics, f, indent=2, default=lambda x: x.item() if hasattr(x, "item") else str(x))

    print("Wygenerowano rozszerzony raport diagnostyczny: data/diagnostic_report.json")

run_diagnostics()
