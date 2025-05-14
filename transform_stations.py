import pandas as pd
import json
import re

CHICAGO_LAT_MIN = 41.2
CHICAGO_LAT_MAX = 42.2
CHICAGO_LON_MIN = -88.2
CHICAGO_LON_MAX = -87.2

def load_station_information(path='data/station_information.json'):
    with open(path) as f:
        data = json.load(f)
        df = pd.DataFrame(data)
    print("Dostępne kolumny:", df.columns.tolist())
    return df[['station_id', 'name', 'lat', 'lon', 'capacity']]

def validate_data(df):
    print("Liczba unikalnych stacji:", len(df['station_id'].unique()))
    print("Brakujące wartości:\n", df.isnull().sum())

    df_outside = df[
        (df["lat"] < CHICAGO_LAT_MIN) |
        (df["lat"] > CHICAGO_LAT_MAX) |
        (df["lon"] < CHICAGO_LON_MIN) |
        (df["lon"] > CHICAGO_LON_MAX)
    ]
    print(f"Stacje poza Chicago: {len(df_outside)}")

    uuid_ids = df[df['station_id'].str.contains('-')]
    print(f"Liczba stacji z UUID: {len(uuid_ids)}")
    print(f"Liczba stacji z ID liczbowym: {len(df['station_id'].unique()) - len(uuid_ids)}")

    print("Rozkład capacity:\n", df['capacity'].describe())
    print("Stacje z capacity <= 0:\n", df[df['capacity'] <= 0])

    print("Duplikaty station_id:", df['station_id'].duplicated().sum())
    print("Duplikaty name:", df['name'].duplicated().sum())
    print("Nazwy powtarzające się:\n", df[df['name'].duplicated(keep=False)])

    print("Duplikaty współrzędnych (lat, lon):", df.duplicated(subset=['lat', 'lon']).sum())
    print(df[df.duplicated(subset=['lat', 'lon'], keep=False)])

def normalize_name(name):
    name = name.lower()
    name = re.sub(r'[^a-z0-9& ]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

def process_duplicates(df):
    # Grupowanie stacji o tych samych nazwach i lokalizacji
    df = df.groupby(['name', 'lat', 'lon']).agg({
        'capacity': lambda x: int(x.mean()),
        'station_id': 'first'
    }).reset_index()

    # Normalizacja nazw
    df['normalized_name'] = df['name'].apply(normalize_name)

    # Usuwanie duplikatów na podstawie znormalizowanej nazwy i lokalizacji
    df = df.drop_duplicates(subset=['normalized_name', 'lat', 'lon'])
    df = df.drop(columns=['normalized_name'])

    return df

def save_station_data(df, path='data/station_information_final.json'):
    df.to_json(path, orient="records", indent=2)
    print(f"Dane zapisane do {path}")
    print("Liczba unikalnych station_id po przetworzeniu:", len(df['station_id'].unique()))


df = load_station_information()
validate_data(df)
df = process_duplicates(df)
save_station_data(df)
