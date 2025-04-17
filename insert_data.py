import pandas as pd
import mysql.connector
import json
import numpy as np
from datetime import datetime, timezone

# Wczytanie konfiguracji z pliku JSON
with open("DB_CONFIG.json", "r") as config_file:
    DB_CONFIG = json.load(config_file)
def insert_station_information():
    df = pd.read_json("data/station_information.json")

    df = df[['station_id', 'name', 'lat', 'lon', 'capacity']]

    df = df.where(pd.notnull(df), None)

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            cursor.execute('''
                INSERT INTO station_information (station_id, name, lat, lon, capacity)
                VALUES (%s, %s, %s, %s, %s)
            ''', (row['station_id'], row['name'], row['lat'], row['lon'], row['capacity']))
        except mysql.connector.errors.IntegrityError:
            pass

    conn.commit()
    cursor.close()
    conn.close()

    print("Dane ze station_information.json zostały zapisane do tabeli 'stations'.")


def insert_vehicle_types():
    df = pd.read_json("data/vehicle_types.json")

    df = df[['vehicle_type_id', 'form_factor', 'propulsion_type', 'max_range_meters']]

    df = df.replace({np.nan: None})

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            cursor.execute('''
                INSERT INTO vehicle_types (vehicle_type_id, form_factor, propulsion_type, max_range_meters)
                VALUES (%s, %s, %s, %s)
            ''', (
                row['vehicle_type_id'],
                row['form_factor'],
                row['propulsion_type'],
                row['max_range_meters']
            ))
        except mysql.connector.errors.IntegrityError:
            pass

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Dane z vehicle_types.json zostały zapisane do tabeli 'vehicle_types'.")


def insert_station_status():
    df = pd.read_json("data/station_status.json")

    df = df.replace({np.nan: None})

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        date = datetime.fromtimestamp(row['last_reported'], timezone.utc).strftime('%Y-%m-%d')

        try:
            query = '''
                INSERT INTO station_status (
                    station_id,
                    date,  -- Dodajemy kolumnę 'date'
                    last_reported,
                    num_docks_available,
                    num_bikes_available,
                    num_scooters_available,
                    num_ebikes_available,
                    num_bikes_disabled,
                    num_docks_disabled,
                    num_scooters_unavailable,
                    is_installed,
                    is_renting,
                    is_returning
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            values = (
                row['station_id'],
                date,
                row['last_reported'],
                row.get('num_docks_available', 0),
                row.get('num_bikes_available', 0),
                row.get('num_scooters_available', 0),
                row.get('num_ebikes_available', 0),
                row.get('num_bikes_disabled', 0),
                row.get('num_docks_disabled', 0),
                row.get('num_scooters_unavailable', 0),
                row.get('is_installed', 0),
                row.get('is_renting', 0),
                row.get('is_returning', 0)
            )

            cursor.execute(query, values)
        except mysql.connector.errors.IntegrityError as e:
            print("IntegrityError:", e)
            pass
        except Exception as e:
            print("Error:", e)

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Dane z station_status.json zostały zapisane do tabeli 'station_status'.")


insert_vehicle_types()
insert_station_information()
insert_station_status()
