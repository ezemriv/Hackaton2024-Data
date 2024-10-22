# src/data_preprocessing/cleaner.py

import pandas as pd
import numpy as np
import geopandas as gpd
import os

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
        gdf['lon'] = centroids.geometry.x
        gdf['lat'] = centroids.geometry.y

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

        return gdf
    
    def process_sites_data(self, interest_path, interest_encoding):
        
        """
        Load dataset as pandas DataFrame
        """

        df = pd.read_csv(interest_path, encoding=interest_encoding)

        # Create 'address' column by combining relevant fields
        df['address'] = df['addresses_road_name'] + " " + df['addresses_start_street_number'].astype(str) + \
                        " " + df['addresses_end_street_number'].fillna('').astype(str)
        
        # Select specific columns
        cols_to_keep = ['name', 'values_category', 'address', 'geo_epgs_4326_lat', 'geo_epgs_4326_lon']
        df = df[cols_to_keep]
        
        # Rename columns
        df = df.rename(columns={'name': 'name', 'values_category': 'category', 'geo_epgs_4326_lat': 'lat', 'geo_epgs_4326_lon': 'lon'})
        
        # Define categories to extract
        categories = ['Centre', 'Parc', 'Jardins', 'Parròquia', 'Museu', 'Mercat', 'Escola', 'Palau', 'Platja', 'Club', 'Cementiri', 'Restaurant']
        
        # Function to extract category from name
        def extract_category(name):
            for category in categories:
                if category.lower() in name.lower():
                    return category
            return 'otro'
        
        # Apply the category extraction function
        df['category'] = df['name'].apply(extract_category)

        return df

    def initial_processing(self, concentration_file_path, interest_path, interest_encoding, clean_data_path):
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
        df_sites.to_csv(sites_clean_path, index=False)

