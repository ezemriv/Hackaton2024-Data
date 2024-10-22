import sys
import os
import yaml
import pandas as pd
import numpy as np

from src.data_preprocessing.cleaner import DataLoader

def load_config(config_file='configs/config.yaml'):
    """Loads configuration parameters from a YAML file."""
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def process_data(config):
    """Processes data using paths and encoding from the config."""
    
    # Initialize the DataLoader
    data_loader = DataLoader()
    
    # Extract paths and encoding from the config
    concentration_file_path = config['data_paths']['concentration_file']  # full path inside the config already
    sites_file_path = config['data_paths']['sites_file']  # full path inside the config already
    clean_data_path = config['data_paths']['clean_data_folder']  # clean_data_folder path
    interest_encoding = config['data_paths']['interest_encoding']  # encoding for the sites file
    sound_path1 = config['data_paths']['sound_file1']  # full path inside the config already
    sound_path2 = config['data_paths']['sound_file2']  # full path inside the config already
    ids_path = config['data_paths']['ids_file']  # full path inside the config already

    # Ensure the clean data folder exists
    os.makedirs(clean_data_path, exist_ok=True)

    # Process data using DataLoader methods
    data_loader.initial_processing(
        concentration_file_path=concentration_file_path,
        interest_path=sites_file_path,
        interest_encoding=interest_encoding,
        sound_path1=sound_path1,
        sound_path2=sound_path2,
        ids_path=ids_path,
        clean_data_path=clean_data_path
    )

def main():
    # Load configuration
    config = load_config()

    # Process data
    process_data(config)

if __name__ == '__main__':
    main()