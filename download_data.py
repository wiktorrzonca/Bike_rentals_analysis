import requests
import json

def download_data(endpoint_url):
    # Wykonanie zapytania HTTP GET
    response = requests.get(endpoint_url)

    # Sprawdzenie, czy zapytanie zakończyło się sukcesem
    if response.status_code == 200:
        # Pobranie danych w formacie JSON
        data = response.json()

        # Zapisanie danych do pliku JSON
        with open('data.json', 'w') as json_file:
            json.dump(data, json_file, indent=4)

        print(f"Dane zostały zapisane do pliku 'data.json'.")
    else:
        print(f"Error: {response.status_code}")


def collect_keys(data, parent_key='', keys_set=None):
    """Zbiera unikalne klucze z JSON-a bez powtarzania."""
    if keys_set is None:
        keys_set = set()

    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            keys_set.add(new_key)  # Dodajemy klucz do zbioru
            collect_keys(value, new_key, keys_set)  # Rekurencja
    elif isinstance(data, list):
        # Zbieramy klucze z elementów listy tylko raz
        if len(data) > 0:
            collect_keys(data[0], parent_key, keys_set)  # Rekurencja tylko dla pierwszego elementu

    return keys_set


# Przykładowe użycie z różnym endpointem
endpoint_url = ""  # Możesz zmienić ten URL
download_data(endpoint_url)

# Odczytanie danych z pliku JSON
with open('data.json', 'r') as file:
    data = json.load(file)

# Zbieranie kluczy i wypisywanie
keys = collect_keys(data)
for key in sorted(keys):
    print(key)
