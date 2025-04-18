import os
import pandas as pd
import requests

def save_to_json(df, filename, folder="data"):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    df.to_json(path, orient="records", indent=2)

def load_csv_data(file_path):
    return pd.read_csv(file_path)

def fetch_api_data(url, key_path):
    response = requests.get(url).json()
    data = response
    for key in key_path:
        data = data[key]
    return pd.DataFrame(data)

def load_all_data():
    urls = {
        "station_information": ("https://gbfs.lyft.com/gbfs/2.3/chi/en/station_information.json", ["data", "stations"]),
        "station_status": ("https://gbfs.lyft.com/gbfs/2.3/chi/en/station_status.json", ["data", "stations"]),
        "vehicle_types": ("https://gbfs.lyft.com/gbfs/2.3/chi/en/vehicle_types.json", ["data", "vehicle_types"]),
    }

    dataframes = {}
    for name, (url, path) in urls.items():
        df = fetch_api_data(url, path)
        dataframes[name] = df
        save_to_json(df, f"{name}.json")

    trip_path = "202502.csv"
    df_trip = load_csv_data(trip_path)
    dataframes["trip_history"] = df_trip
    save_to_json(df_trip, "trip_history.json")

    return dataframes


load_all_data()