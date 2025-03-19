import pandas as pd
import mysql.connector
import json

# Wczytanie konfiguracji z pliku JSON
with open("DB_CONFIG.json", "r") as config_file:
    DB_CONFIG = json.load(config_file)

# Wczytanie pliku CSV
df = pd.read_csv("test.csv")

# Sprawdzenie, czy są NaN w danych
print("Przed zamianą NaN:")
print(df.isna().sum())  # Wyświetli liczbę NaN w każdej kolumnie

# Zamiana NaN na wartość domyślną (np. 0, lub None dla NULL w MySQL)
df = df.fillna(0)  # Możesz tu zamienić 0 na inną wartość, np. ''

# Sprawdzenie, czy NaN zostały zamienione
print("Po zamianie NaN:")
print(df.isna().sum())  # Teraz wszystkie NaN powinny zostać zamienione na 0 lub inną wartość

# Połączenie z bazą danych
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# Tworzenie tabeli (jeśli nie istnieje)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS rides (
        ride_id VARCHAR(255) PRIMARY KEY,
        rideable_type VARCHAR(50),
        started_at DATETIME,
        ended_at DATETIME,
        start_station_name VARCHAR(255),
        start_station_id VARCHAR(50),
        end_station_name VARCHAR(255),
        end_station_id VARCHAR(50),
        start_lat DOUBLE,
        start_lng DOUBLE,
        end_lat DOUBLE,
        end_lng DOUBLE,
        member_casual VARCHAR(50)
    )
''')

# Wstawianie danych do tabeli
for _, row in df.iterrows():
    cursor.execute('''
        INSERT INTO rides (
            ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, 
            end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', tuple(row))

# Zatwierdzenie i zamknięcie połączenia
conn.commit()
cursor.close()
conn.close()

print("Dane zostały zapisane do bazy danych.")
