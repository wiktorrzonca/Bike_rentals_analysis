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

    # === NAJPOPULARNIEJSZE STACJE STARTOWE I KOŃCOWE ===
    top_start = query("""
        SELECT si.name AS start_station_name, COUNT(*) AS start_count
        FROM trip_history th
        JOIN station_information si ON th.start_station_id = si.station_id
        GROUP BY start_station_name
        ORDER BY start_count DESC
        LIMIT 10
    """)
    diagnostics["top_start_stations"] = top_start.to_dict(orient="records")

    top_end = query("""
        SELECT si.name AS end_station_name, COUNT(*) AS end_count
        FROM trip_history th
        JOIN station_information si ON th.end_station_id = si.station_id
        GROUP BY end_station_name
        ORDER BY end_count DESC
        LIMIT 10
    """)
    diagnostics["top_end_stations"] = top_end.to_dict(orient="records")

    # === TYPY ROWERÓW – WYKORZYSTANIE ===
    bike_types = query("""
        SELECT vt.form_factor, COUNT(*) AS count
        FROM trip_history th
        JOIN vehicle_types vt ON th.vehicle_type_id = vt.vehicle_type_id
        GROUP BY vt.form_factor
    """)
    diagnostics["bike_type_usage"] = bike_types.set_index("form_factor")["count"].to_dict()

    # === TYP UŻYTKOWNIKA ===
    membership = query("""
        SELECT user_type, COUNT(*) AS count
        FROM trip_history
        GROUP BY user_type
    """)
    diagnostics["membership_distribution"] = membership.set_index("user_type")["count"].to_dict()

    # === ŚREDNI CZAS PODRÓŻY PER TYP POJAZDU ===
    duration_by_type = query("""
        SELECT vt.form_factor, AVG(duration_seconds) AS avg_duration
        FROM trip_history th
        JOIN vehicle_types vt ON th.vehicle_type_id = vt.vehicle_type_id
        GROUP BY vt.form_factor
    """)
    diagnostics["avg_duration_by_bike_type"] = duration_by_type.set_index("form_factor")["avg_duration"].round(2).to_dict()

    # === NAJWIĘCEJ PRZEJAZDÓW DZIENNIE ===
    busiest_days = query("""
        SELECT DATE(start_time) as ride_date, COUNT(*) as ride_count
        FROM trip_history
        GROUP BY ride_date
        ORDER BY ride_count DESC
        LIMIT 5
    """)
    diagnostics["busiest_days"] = busiest_days.to_dict(orient="records")

    # === PODZIAŁ NA DZIEŃ I NOC ===
    night_vs_day = query("""
        SELECT 
            CASE 
                WHEN HOUR(start_time) BETWEEN 6 AND 21 THEN 'day'
                ELSE 'night'
            END as period,
            COUNT(*) as count
        FROM trip_history
        GROUP BY period
    """)
    diagnostics["day_vs_night_rides"] = night_vs_day.set_index("period")["count"].to_dict()

    # === DNI TYGODNIA ===
    rides_by_weekday = query("""
        SELECT 
            DAYOFWEEK(start_time) AS weekday,
            COUNT(*) AS count
        FROM trip_history
        GROUP BY weekday
    """)
    days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
    rides_by_weekday["weekday"] = rides_by_weekday["weekday"].apply(lambda x: days[x-1])
    diagnostics["rides_by_weekday"] = rides_by_weekday.set_index("weekday")["count"].to_dict()

    # === ZAPIS ===
    with open("data/diagnostic_report.json", "w") as f:
        json.dump(diagnostics, f, indent=2, default=lambda x: x.item() if hasattr(x, "item") else str(x))

    print("Wygenerowano raport diagnostyczny: data/diagnostic_report.json")

if __name__ == "__main__":
    run_diagnostics()
