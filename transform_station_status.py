import pandas as pd
import json

def load_station_status(path='data/station_status.json'):
    with open(path) as f:
        data = json.load(f)
        return pd.DataFrame(data)

def filter_operational_stations(df):
    cols = ["is_renting", "is_returning", "is_installed"]
    for col in cols:
        print(f"\nWartości dla kolumny: {col}")
        print(df[col].value_counts())

    non_operational = df[
        (df["is_renting"] == 0) |
        (df["is_returning"] == 0) |
        (df["is_installed"] == 0)
    ]
    print(non_operational[["station_id", "is_renting", "is_returning", "is_installed"]])

    return df[
        (df["is_renting"] == 1) &
        (df["is_returning"] == 1) &
        (df["is_installed"] == 1)
    ]

def clean_station_availability(df):
    df = df[['station_id', 'num_bikes_available', 'num_ebikes_available', 'num_scooters_available']]
    print(df)
    print(df.isnull().sum())

    df["num_scooters_available"].fillna(0, inplace=True)

    df = df.rename(columns={
        'num_bikes_available': 'average_bikes_available',
        'num_ebikes_available': 'average_ebikes_available',
        'num_scooters_available': 'average_scooters_available'
    })
    print(df)

    return df

def enrich_with_trip_data(df, trip_path='data/trip_history_final.json'):
    with open(trip_path) as f:
        trips = json.load(f)
        df_trips = pd.DataFrame(trips)

    rents = df_trips['start_station_id'].value_counts()
    returns = df_trips['end_station_id'].value_counts()

    df['total_rents'] = df['station_id'].map(rents).fillna(0).astype(int)
    df['total_returns'] = df['station_id'].map(returns).fillna(0).astype(int)

    print(df)
    print("Suma wypożyczeń:", sum(df['total_rents']))
    return df

def save_final_station_status(df, path='data/station_status_final.json'):
    df.to_json(path, orient="records", indent=2)
    print(f"Dane zapisane do {path}")


df = load_station_status()
df = filter_operational_stations(df)
df = clean_station_availability(df)
df = enrich_with_trip_data(df)
save_final_station_status(df)

