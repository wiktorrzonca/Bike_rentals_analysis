import pandas as pd
import json

with open('data/station_information.json') as f:
    data = json.load(f)
    df = pd.DataFrame(data)

print(df.columns)
df = df[['station_id', 'name', 'lat', 'lon', 'capacity']]
print(df.columns)
nan_count = df.isnull().sum()

print(nan_count)
print(len(df['station_id'].unique()))

CHICAGO_LAT_MIN = 41.2
CHICAGO_LAT_MAX = 42.2
CHICAGO_LON_MIN = -88.2
CHICAGO_LON_MAX = -87.2

df_outside_chicago = df[
    (df["lat"] < CHICAGO_LAT_MIN) |
    (df["lat"] > CHICAGO_LAT_MAX) |
    (df["lon"] < CHICAGO_LON_MIN) |
    (df["lon"] > CHICAGO_LON_MAX)
]

print(f"outside of Chicago and surrounding area: {len(df_outside_chicago)}")

uuid_ids = df[df['station_id'].str.contains('-')]
print(f"Liczba stacji z UUID: {len(uuid_ids)}")
print(f"Liczba stacji z liczbami: {len(df['station_id'].unique()) - len(uuid_ids)}")

print(df['capacity'].describe())
print(df[df['capacity'] <= 0])  # Stacje o pojemności 0 lub ujemnej

print(df['station_id'].duplicated().sum())
print(df['name'].duplicated().sum())

print(df[df['name'].duplicated(keep=False)])

print(df.duplicated(subset=['lat', 'lon']).sum())
print(df[df.duplicated(subset=['lat', 'lon'], keep=False)])

df = df.groupby(['name', 'lat', 'lon']).agg({
    'capacity': lambda x: int(x.mean()),  # średnia capacity -> int
    'station_id': 'first'  # zachowaj jeden station_id, możesz później poprawić
}).reset_index()

import pandas as pd
import re

def normalize_name(name):
    # Małe litery, usuwanie znaków specjalnych i nadmiarowych spacji
    name = name.lower()
    name = re.sub(r'[^a-z0-9& ]', '', name)  # usuwa wszystko poza literami, cyframi, spacjami i '&'
    name = re.sub(r'\s+', ' ', name)  # redukuje wielokrotne spacje do jednej
    name = name.strip()
    return name

df['normalized_name'] = df['name'].apply(normalize_name)

# Teraz usuń duplikaty bazując na (normalized_name, lat, lon)
df = df.drop_duplicates(subset=['normalized_name', 'lat', 'lon'])

# Na koniec można usunąć kolumnę pomocniczą
df = df.drop(columns=['normalized_name'])

print(df)

print(len(df['station_id'].unique()))


df.to_json("data/station_information_final.json", orient="records", indent=2)

# import matplotlib.pyplot as plt
# plt.figure(figsize=(10, 8))
# plt.scatter(df['lon'], df['lat'], c=df['capacity'], cmap='viridis', s=10)
# plt.colorbar(label='Capacity')
# plt.xlabel('Longitude')
# plt.ylabel('Latitude')
# plt.title('Station locations colored by capacity')
# plt.show()
