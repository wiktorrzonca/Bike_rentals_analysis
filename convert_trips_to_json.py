import os
import pandas as pd

def save_to_json(df, filename, folder="data"):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    df.to_json(path, orient="records", indent=2)

def load_csv_data(file_path):
    return pd.read_csv(file_path)

def convert_trips_to_json(input_folder="data/trip_history_data", output_folder="data"):
    os.makedirs(output_folder, exist_ok=True)
    all_dfs = []

    files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]
    for file in files:
        file_path = os.path.join(input_folder, file)
        df = load_csv_data(file_path)
        all_dfs.append(df)
        print(f"Wczytano: {file}")

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        save_to_json(combined_df, "trip_history.json", folder=output_folder)
        print(f"Zapisano połączone dane do: {os.path.join(output_folder, 'trip_history.json')}")
    else:
        print("Nie znaleziono żadnych plików CSV do przetworzenia.")

convert_trips_to_json()
