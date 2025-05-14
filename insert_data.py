import pandas as pd
import mysql.connector
import json
import numpy as np
from datetime import datetime, timezone
import pytz

with open("DB_CONFIG.json", "r") as config_file:
    DB_CONFIG = json.load(config_file)

def insert_station_information():
    df = pd.read_json("data/station_information_final.json")
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
    print("Dane ze station_information.json zostały zapisane do tabeli 'station_information'.")

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
    print("Dane z vehicle_types.json zostały zapisane do tabeli 'vehicle_types'.")

def insert_station_status():
    df = pd.read_json("data/station_status_final.json")
    df = df.replace({np.nan: None})

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Filtruj tylko poprawne station_id istniejące w station_information
    cursor.execute("SELECT station_id FROM station_information")
    valid_ids = set(row[0] for row in cursor.fetchall())
    df = df[df['station_id'].isin(valid_ids)]

    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    for _, row in df.iterrows():
        try:
            query = '''
                INSERT INTO station_status (
                    station_id,
                    date,
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
                today_str,
                0,
                0,
                row.get('average_bikes_available', 0),
                row.get('average_scooters_available', 0),
                row.get('average_ebikes_available', 0),
                0, 0, 0,
                1, 1, 1
            )
            cursor.execute(query, values)
        except mysql.connector.errors.IntegrityError as e:
            print("IntegrityError:", e)
        except Exception as e:
            print("Error:", e)

    conn.commit()
    cursor.close()
    conn.close()
    print("Dane ze station_status_final.json zostały zapisane do tabeli 'station_status'.")

def insert_trip_history():
    df = pd.read_json("data/trip_history_final.json")
    df = df.replace({np.nan: None})

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    chicago_tz = pytz.timezone("America/Chicago")

    for _, row in df.iterrows():
        try:
            started_at = pd.to_datetime(row['started_at']).tz_localize('UTC').astimezone(chicago_tz)
            ended_at = pd.to_datetime(row['ended_at']).tz_localize('UTC').astimezone(chicago_tz)

            # Obetnij mikrosekundy i przekształć do stringa
            started_at_str = started_at.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            ended_at_str = ended_at.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

            query = '''
                INSERT INTO trip_history (
                    trip_id,
                    start_time,
                    end_time,
                    duration_seconds,
                    start_station_id,
                    end_station_id,
                    vehicle_type_id,
                    user_type,
                    distance_meters
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            values = (
                row['ride_id'],
                started_at_str,
                ended_at_str,
                int(row['trip_duration_sec']),
                row['start_station_id'],
                row['end_station_id'],
                row['rideable_type'],
                row['member_casual'],
                None
            )

            cursor.execute(query, values)

        except mysql.connector.errors.IntegrityError as e:
            print("IntegrityError:", e)
        except Exception as e:
            print("Error:", e)

    conn.commit()
    cursor.close()
    conn.close()
    print("Dane z trip_history_final.json zostały zapisane do tabeli 'trip_history'.")


# Kolejność ma znaczenie:
insert_vehicle_types()
insert_station_information()
insert_station_status()
insert_trip_history()
