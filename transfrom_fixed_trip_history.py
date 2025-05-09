import pandas as pd
import json

with open('data/trip_history_fixed.json') as f:
    data = json.load(f)
    df = pd.DataFrame(data)

df['started_at'] = pd.to_datetime(df['started_at'])
df['ended_at'] = pd.to_datetime(df['ended_at'])

df['trip_duration_sec'] = (df['ended_at'] - df['started_at']).dt.total_seconds()

df['trip_duration_min'] = df['trip_duration_sec'] / 60

df = df[df['trip_duration_sec'] >= 0]

df.to_json("data/trip_history_final.json", orient="records", indent=2)
