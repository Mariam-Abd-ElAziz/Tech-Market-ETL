import pandas as pd
import os

def extract_csv(data_directory):
    csv_data = {}
    for filename in os.listdir(data_directory):
        if filename.endswith('.csv'):
            full_path = os.path.join(data_directory, filename)
            csv_data[filename.replace('.csv', '')] = pd.read_csv(full_path)
    return csv_data

