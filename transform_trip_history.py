import pandas as pd
import json
from math import radians, cos, sin, asin, sqrt
from scipy.spatial import cKDTree  # Importujemy KD-Tree z scipy

with open('data/trip_history.json') as f:
    trip_data = json.load(f)
    df_trips = pd.DataFrame(trip_data)

df_trips = df_trips[['ride_id', 'started_at', 'ended_at',
                     'start_station_id', 'end_station_id',
                     'rideable_type', 'member_casual',
                     'start_lat', 'start_lng', 'end_lat', 'end_lng']]


print(df_trips.isna().sum())

df_trips.dropna(subset=["start_lat", "start_lng", "end_lat", "end_lng"], inplace=True)

with open('data/station_information.json') as f:
    station_data = json.load(f)
    df_stations = pd.DataFrame(station_data)

valid_id_stations = set(df_stations['station_id'])

station_coords = df_stations[['lat', 'lon']].values
station_tree = cKDTree(station_coords)

def find_nearest_station(lat, lon):
    dist, idx = station_tree.query([lat, lon], k=1)
    return df_stations.iloc[idx]["station_id"]


missing_start = ~df_trips["start_station_id"].isin(valid_id_stations)
df_trips.loc[missing_start, "start_station_id"] = df_trips[missing_start].apply(
    lambda row: find_nearest_station(row["start_lat"], row["start_lng"]), axis=1)

missing_end = ~df_trips["end_station_id"].isin(valid_id_stations)
df_trips.loc[missing_end, "end_station_id"] = df_trips[missing_end].apply(
    lambda row: find_nearest_station(row["end_lat"], row["end_lng"]), axis=1)

print(df_trips[['start_station_id', 'end_station_id']])

df_trips.to_json("data/trip_history_fixed.json", orient="records", indent=2)
