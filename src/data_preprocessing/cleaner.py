# src/data_preprocessing/cleaner.py

import pandas as pd
import numpy as np
import geopandas as gpd
import os
import re

class DataLoader():
    def __init__(self):
       pass

    def process_gpkg_data(self, concentration_file_path):
        
        """
        Load dataset as pandas DataFrame
        """

        # Step 1: Read the GeoDataFrame
        gdf = gpd.read_file(concentration_file_path)

        # Step 3: Calculate centroids
        gdf['centroid'] = gdf.geometry.centroid

        # Step 4: Reproject centroids to WGS84
        # Create a GeoDataFrame with centroids as geometry
        centroids = gdf.copy()
        centroids = centroids.set_geometry('centroid')
        centroids = centroids.to_crs(epsg=4326)

        # Step 5: Extract longitude and latitude
        gdf['lon'] = round(centroids.geometry.x, 3)
        gdf['lat'] = round(centroids.geometry.y, 3)

        # Create dummy date column for every day in 2019
        gdf['date'] = pd.date_range(start='2019-01-01', end='2019-12-31', freq='D').to_series().sample(len(gdf), replace=True).values
        gdf['day_of_week'] = gdf['date'].dt.day_name()
        gdf['month_name'] = gdf['date'].dt.month_name()

        gdf.rename(columns={'DN':'id'}, inplace=True)

        select = [
            'lon',
            'lat',
            'date', 
            'day_of_week',
            'month_name'
        ]

        gdf = gdf[select]

        print(f"Final shape of turistic concentration data: {gdf.shape}")
        return gdf
    
    def process_sites_data(self, interest_path, interest_encoding):
        
        """
        Load dataset as pandas DataFrame
        """

        df = pd.read_csv(interest_path, encoding=interest_encoding)

        # Create 'address' column by combining relevant fields
        df['address'] = df['addresses_road_name'] + " " + df['addresses_start_street_number'].astype(str)
        df['address'] = df['address'].str.replace(r'\.0', '', regex=True)
        
        # Function to remove special characters manually
        def remove_special_characters(text):
            if isinstance(text, str):  # Proceed only if the text is a string
                # Replace commas, dashes, quotes, and other common special characters with an empty string
                return re.sub(r'[,\-"\(\)]', '', text)
            return text  # Return the original text if it's not a string
        
        # Apply the function to the 'address' column
        df['address'] = df['address'].apply(remove_special_characters)
        df['name'] = df['name'].apply(remove_special_characters)
    
        # Select specific columns
        select = ['name', 'values_category', 'address', 'geo_epgs_4326_lat', 'geo_epgs_4326_lon']
        df = df[select]
        
        # Rename columns
        df = df.rename(columns={'name': 'name', 'values_category': 'category', 'geo_epgs_4326_lat': 'lat', 'geo_epgs_4326_lon': 'lon'})
        
        # Round coordinates columns
        df['lat'] = df['lat'].round(3)
        df['lon'] = df['lon'].round(3)

        # Define categories to extract (with important Barcelona sites)
        categories = ['Centre', 'Parc', 'Jardins', 'Parròquia', 'Museu', 'Mercat', 'Escola', 'Palau', 'Platja', 'Club', 'Cementiri', 'Restaurant']
        # Define featured (important) categories for Barcelona
        featured_categories = ['Sagrada Família', 'Montjuïc', 'El Tibidabo', 'Casa Batlló', 'La Pedrera', 'El Gòtic', 'El Born', 'El Raval', 'Barceloneta']

        # Function to extract category from name
        def extract_category(name):

            # Check in both categories and featured categories
            for category in categories + featured_categories:
                if category.lower() in name.lower():
                    if category in featured_categories:
                        return 'destacat'  # Catalan for "highlight"
                    return category
            return 'otro'
        
        # Apply the category extraction function
        df['category'] = df['name'].apply(extract_category)
        df.dropna(how='any', inplace=True)

        print(f"Final shape of turistic sites data: {df.shape}")
        return df

    def process_sound_data(self, sound_path, ids_path):
        
        """
        Load dataset of sound data and merges with ids location as pandas DataFrame
        """

        # Load and processsound data
        sound_df = pd.read_csv(sound_path)        
        sound_df['date'] = sound_df['Any'].astype(str) + '-' + sound_df['Mes'].astype(str) + '-' + sound_df['Dia'].astype(str)
        sound_df['date'] = pd.to_datetime(sound_df['date'])

        select = ['Id_Instal', 'Nivell_LAeq_1h', 'date']

        sound_df = sound_df[select]
        sound_df = sound_df.groupby(['Id_Instal', 'date'])['Nivell_LAeq_1h'].mean().reset_index()
        sound_df.rename(columns={'Nivell_LAeq_1h': 'sound_level_mean'}, inplace=True)
        sound_df['sound_level_mean'] = sound_df['sound_level_mean'].round(2)

        # Load and process ids location
        ids_df = pd.read_csv(ids_path)
        select = ['Id_Instal', 'Latitud', 'Longitud']

        ids_df = ids_df[select]

        merged_df = sound_df.merge(ids_df, on='Id_Instal', how='left')
        merged_df.rename(columns={'Latitud': 'lat', 'Longitud': 'lon'}, inplace=True)

        merged_df['lat'] = round(merged_df['lat'], 3)
        merged_df['lon'] = round(merged_df['lon'], 3)

        # merged_df['day_of_week'] = merged_df['date'].dt.day_name()
        # merged_df['month_name'] = merged_df['date'].dt.month_name()

        merged_df.drop('Id_Instal', axis=1, inplace=True)

        merged_df.dropna(inplace=True)

        return merged_df
    
    def initial_processing(self, concentration_file_path, 
                           interest_path, interest_encoding, 
                           sound_path1, sound_path2, ids_path,
                           clean_data_path):
        
        # Process gpkg data
        df_gpkg = self.process_gpkg_data(concentration_file_path)
        
        # Extract the filename and save to clean_data_path
        gpkg_filename = os.path.basename(concentration_file_path)  # Get the filename (e.g., '2019_turisme_allotjament.gpkg')
        gpkg_clean_path = os.path.join(clean_data_path, gpkg_filename.replace('.gpkg', '_clean.csv'))  # Save as a CSV in clean_data folder
        df_gpkg.to_csv(gpkg_clean_path, index=False)

        # Process sites data
        df_sites = self.process_sites_data(interest_path, interest_encoding)
        
        # Extract the filename and save to clean_data_path
        sites_filename = os.path.basename(interest_path)  # Get the filename (e.g., 'opendatabcn_pics-csv.csv')
        sites_clean_path = os.path.join(clean_data_path, sites_filename.replace('.csv', '_clean.csv'))  # Save as a CSV in clean_data folder
        df_sites.to_csv(sites_clean_path, index=False, encoding='utf-8')

        # Process sound data 1
        df_sound_1 = self.process_sound_data(sound_path=sound_path1, ids_path=ids_path)
        df_sound_2 = self.process_sound_data(sound_path=sound_path2, ids_path=ids_path)
        
        merged_sound_data = pd.concat([df_sound_1, df_sound_2], ignore_index=True)

        # Extract the filename and save to clean_data_path
        sound_filename = os.path.basename(sound_path1)
        sound_clean_path = os.path.join(clean_data_path, sound_filename.replace('.csv', '_clean.csv'))  # Save as a CSV in clean_data folder
        merged_sound_data.to_csv(sound_clean_path, index=False)
        print(f"Final shape of sound data: {merged_sound_data.shape}")
        print("Data processing completed successfully!")