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

    # Liczba rekordów w tabelach
    diagnostics["vehicle_types_count"] = int(query("SELECT COUNT(*) FROM vehicle_types").iloc[0, 0])
    diagnostics["station_information_count"] = int(query("SELECT COUNT(*) FROM station_information").iloc[0, 0])
    diagnostics["trip_history_count"] = int(query("SELECT COUNT(*) FROM trip_history").iloc[0, 0])
    diagnostics["station_status_count"] = int(query("SELECT COUNT(*) FROM station_status").iloc[0, 0])

    # Duplikaty station_id w tabeli station_information
    duplicates = query("""
        SELECT station_id, COUNT(*) AS count
        FROM station_information
        GROUP BY station_id
        HAVING count > 1
    """)
    diagnostics["duplicate_station_ids"] = int(len(duplicates))

    # Średnia długość przejazdu
    avg_duration = query("SELECT AVG(duration_seconds) AS avg_duration FROM trip_history")
    diagnostics["avg_trip_duration_sec"] = float(avg_duration.iloc[0, 0] or 0)

    # Statystyki użycia pojazdów
    vehicle_stats = query("""
        SELECT vehicle_type_id, COUNT(*) AS count
        FROM trip_history
        GROUP BY vehicle_type_id
    """)
    diagnostics["vehicle_usage_stats"] = vehicle_stats.to_dict(orient="records")

    # Brakujące stacje startowe
    missing_start = query("""
        SELECT COUNT(*) AS missing FROM trip_history
        WHERE start_station_id IS NULL OR start_station_id NOT IN (SELECT station_id FROM station_information)
    """).iloc[0, 0]
    diagnostics["missing_start_station_id"] = int(missing_start)

    # Brakujące stacje końcowe
    missing_end = query("""
        SELECT COUNT(*) AS missing FROM trip_history
        WHERE end_station_id IS NULL OR end_station_id NOT IN (SELECT station_id FROM station_information)
    """).iloc[0, 0]
    diagnostics["missing_end_station_id"] = int(missing_end)

    conn.close()

    # Zapisz dane do pliku JSON z konwersją typów
    with open("diagnostic_report.json", "w") as f:
        json.dump(diagnostics, f, indent=2, default=lambda x: x.item() if hasattr(x, "item") else str(x))

    print("✅ Wygenerowano raport diagnostyczny: diagnostic_report.json")


if __name__ == "__main__":
    run_diagnostics()
