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
# Tworzymy DataFrame dla startowych i końcowych stacji, zmieniając nazwy kolumn
start_stations = df[['start_station_id', 'start_station_name', 'start_lat', 'start_lng']].drop_duplicates()
start_stations.columns = ['station_id', 'station_name', 'latitude', 'longitude']  # Ujednolicenie nazw kolumn

end_stations = df[['end_station_id', 'end_station_name', 'end_lat', 'end_lng']].drop_duplicates()
end_stations.columns = ['station_id', 'station_name', 'latitude', 'longitude']  # Ujednolicenie nazw kolumn

# Połączenie obu DataFrame i usunięcie duplikatów
stations = pd.concat([start_stations, end_stations]).drop_duplicates(subset=['station_id'])

# Połączenie z bazą danych
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# Tworzenie tabeli 'stations', jeśli nie istnieje
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stations (
        station_id VARCHAR(50) PRIMARY KEY,
        station_name VARCHAR(255),
        latitude DOUBLE,
        longitude DOUBLE
    )
''')

# Wstawianie unikalnych stacji do tabeli 'stations'
for _, row in stations.iterrows():
    try:
        cursor.execute('''
            INSERT INTO stations (station_id, station_name, latitude, longitude)
            VALUES (%s, %s, %s, %s)
        ''', (row['station_id'], row['station_name'], row['latitude'], row['longitude']))
    except mysql.connector.errors.IntegrityError:
        # Ignorowanie duplikatów (stacja już istnieje)
        pass

# Zatwierdzenie zmian w bazie danych
conn.commit()

# Zamknięcie połączenia
cursor.close()
conn.close()

print("Unikalne stacje zostały zapisane do bazy danych.")
