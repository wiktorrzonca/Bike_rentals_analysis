import pandas as pd


def check_missing_data(csv_file):
    # Wczytanie danych z pliku CSV
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"Plik '{csv_file}' nie został znaleziony.")
        return

    # Wypisanie liczby wszystkich wierszy w pliku
    total_rows = len(df)
    print(f"Liczba wszystkich wierszy: {total_rows}")

    # Sprawdzenie, które kolumny zawierają puste dane
    missing_data = df.isnull().sum()

    # Obliczenie liczby brakujących danych
    total_missing = missing_data.sum()

    # Wyświetlenie szczegółów
    print(f"Podsumowanie brakujących danych w pliku '{csv_file}':")
    print(missing_data)
    print(f"Całkowita liczba brakujących danych: {total_missing}")

    # Opcjonalnie, jeśli chcesz zobaczyć, ile wierszy ma brakujące dane
    rows_with_missing = df[df.isnull().any(axis=1)]
    print(f"Liczba wierszy z brakującymi danymi: {len(rows_with_missing)}")


# Przykładowe wywołanie funkcji
check_missing_data('202502.csv')
