import pandas as pd
import os
import pickle

def extract_csv(data_directory,state_file="csv_file_state.pkl"):
    if os.path.exists(state_file):
        with open(state_file, 'rb') as f:
            previous_state = pickle.load(f)
    else:
        previous_state = {}

    current_state = {}
    csv_data = {}

    for filename in os.listdir(data_directory):
        if filename.endswith('.csv'):
            full_path = os.path.join(data_directory, filename)
            last_updated_time = os.path.getmtime(full_path)
            current_state[filename] = last_updated_time
            # Check if file is new or modified
            if filename not in previous_state or previous_state[filename] != last_updated_time:
                csv_data[filename.replace('.csv', '')] = pd.read_csv(full_path)

    return csv_data,current_state


