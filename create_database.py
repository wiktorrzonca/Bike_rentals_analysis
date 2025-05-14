import mysql.connector
import json

def create_bike_rentals_schema(config_path="DB_CONFIG.json"):
    # Wczytaj konfigurację bazy z pliku JSON
    with open(config_path, "r") as file:
        db_config = json.load(file)

    # Połącz się bezpośrednio z MySQL (bez wskazywania konkretnej bazy)
    temp_config = db_config.copy()
    temp_config.pop("database", None)

    try:
        conn = mysql.connector.connect(**temp_config)
        cursor = conn.cursor()

        # Tworzenie bazy danych
        cursor.execute("CREATE DATABASE IF NOT EXISTS bike_rentals")
        cursor.execute("USE bike_rentals")

        # Tworzenie tabel
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vehicle_types (
                vehicle_type_id VARCHAR(50) PRIMARY KEY,
                form_factor VARCHAR(50) NOT NULL,
                propulsion_type VARCHAR(50) NOT NULL,
                max_range_meters FLOAT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS station_information (
                station_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                lat FLOAT NOT NULL,
                lon FLOAT NOT NULL,
                capacity INT NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trip_history (
                trip_id VARCHAR(20) PRIMARY KEY,
                start_time DATETIME NOT NULL,
                end_time DATETIME NOT NULL,
                duration_seconds INT NOT NULL,
                start_station_id VARCHAR(50),
                end_station_id VARCHAR(50),
                vehicle_type_id VARCHAR(50) NOT NULL,
                user_type VARCHAR(50),
                distance_meters FLOAT,
                FOREIGN KEY (start_station_id) REFERENCES station_information(station_id),
                FOREIGN KEY (end_station_id) REFERENCES station_information(station_id),
                FOREIGN KEY (vehicle_type_id) REFERENCES vehicle_types(vehicle_type_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS station_status (
                station_id VARCHAR(50),
                date DATE NOT NULL,
                num_bikes_available INT,
                num_ebikes_available INT,
                num_scooters_available FLOAT,
                num_bikes_disabled INT,
                num_scooters_unavailable FLOAT,
                num_docks_available INT,
                num_docks_disabled INT,
                is_renting INT,
                is_returning INT,
                last_reported INT,
                is_installed INT,
                PRIMARY KEY (station_id, date),
                FOREIGN KEY (station_id) REFERENCES station_information(station_id)
            )
        """)

        conn.commit()
        print("Struktura bazy danych 'bike_rentals' została utworzona.")
    except mysql.connector.Error as err:
        print(f"Błąd: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Przykładowe wywołanie
create_bike_rentals_schema()
