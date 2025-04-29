import pandas as pd
import json

with open('data/trip_history_fixed.json') as f:
    trip_data = json.load(f)
    df = pd.DataFrame(trip_data)

print(df.isna().sum())
