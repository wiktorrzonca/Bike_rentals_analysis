import pandas as pd
import json
from scipy.spatial import cKDTree

# Wczytanie i przetwarzanie trip_history.json
with open('data/trip_history.json') as f:
    trip_data = json.load(f)
    df_trips = pd.DataFrame(trip_data)

# Wybór kolumn i usunięcie rekordów bez współrzędnych
df_trips = df_trips[['ride_id', 'started_at', 'ended_at',
                     'start_station_id', 'end_station_id',
                     'rideable_type', 'member_casual',
                     'start_lat', 'start_lng', 'end_lat', 'end_lng']]

df_trips.dropna(subset=["start_lat", "start_lng", "end_lat", "end_lng"], inplace=True)

# Wczytanie danych o stacjach
with open('data/station_information_final.json') as f:
    station_data = json.load(f)
    df_stations = pd.DataFrame(station_data)

valid_id_stations = set(df_stations['station_id'])
station_coords = df_stations[['lat', 'lon']].values
station_tree = cKDTree(station_coords)

# Znajdowanie najbliższej stacji
def find_nearest_station(lat, lon):
    dist, idx = station_tree.query([lat, lon], k=1)
    return df_stations.iloc[idx]["station_id"]

# Uzupełnianie brakujących ID stacji
missing_start = ~df_trips["start_station_id"].isin(valid_id_stations)
df_trips.loc[missing_start, "start_station_id"] = df_trips[missing_start].apply(
    lambda row: find_nearest_station(row["start_lat"], row["start_lng"]), axis=1)

missing_end = ~df_trips["end_station_id"].isin(valid_id_stations)
df_trips.loc[missing_end, "end_station_id"] = df_trips[missing_end].apply(
    lambda row: find_nearest_station(row["end_lat"], row["end_lng"]), axis=1)

# Obliczanie czasu trwania podróży
df_trips['started_at'] = pd.to_datetime(df_trips['started_at'])
df_trips['ended_at'] = pd.to_datetime(df_trips['ended_at'])
df_trips['trip_duration_sec'] = (df_trips['ended_at'] - df_trips['started_at']).dt.total_seconds()
df_trips['trip_duration_min'] = df_trips['trip_duration_sec'] / 60

# Filtrowanie błędnych przejazdów
df_trips = df_trips[df_trips['trip_duration_sec'] >= 0]

# Mapowanie rideable_type
df_trips['rideable_type'] = df_trips['rideable_type'].astype(str).str.strip()
type_mapping = {
    "classic_bike": 1,
    "electric_bike": 2,
    "electric_scooter": 3,
}
df_trips['rideable_type'] = df_trips['rideable_type'].map(type_mapping).fillna(df_trips['rideable_type'])

# Zapis do pliku
df_trips.to_json("data/trip_history_final.json", orient="records", indent=2)
