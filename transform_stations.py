import pandas as pd
import json

with open('data/station_information.json') as f:
    data = json.load(f)
    df = pd.DataFrame(data)

print(df.columns)
df = df[['station_id', 'name', 'lat', 'lon', 'capacity']]
print(df.columns)
print(df.describe)
nan_count = df.isnull().sum()

print(nan_count)
print(len(df['station_id'].unique()))

# Współrzędne Chicago i okolic
CHICAGO_LAT_MIN = 41.2
CHICAGO_LAT_MAX = 42.2
CHICAGO_LON_MIN = -88.2
CHICAGO_LON_MAX = -87.2

# Filtracja danych, które znajdują się poza tym obszarem
df_outside_chicago = df[
    (df["lat"] < CHICAGO_LAT_MIN) |
    (df["lat"] > CHICAGO_LAT_MAX) |
    (df["lon"] < CHICAGO_LON_MIN) |
    (df["lon"] > CHICAGO_LON_MAX)
]

# Wyświetlanie liczby danych poza obszarem Chicago
print(f"outside of Chicago and surrounding area: {len(df_outside_chicago)}")


