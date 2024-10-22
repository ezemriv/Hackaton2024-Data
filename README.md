# 🗂️ Data Processing for Sustainable Tourism Project - Hackathon 2024

## 📖 Overview

This README focuses on the **data processing** part of a larger project that includes both backend and frontend components. The goal is to process datasets that will feed into our **map application** showing:

- Tourist density for the year **2019**
- **Noise levels** across Barcelona for the year **2019**
- **Points of interest** for tourists

This project serves as the backbone for ensuring data is clean and ready for the backend and frontend teams to build their respective components. The final app helps users visualize tourist activity and noise pollution in the city.

## 🗂️ Project Structure

```bash
├── clean_data/                 # Cleaned datasets ready for analysis
├── configs/
│   └── config.yaml             # Configuration file with settings
├── data/                       # Raw data from Open Data Barcelona
├── notebooks/# Jupyter notebook for data exploration and cleaning
├── src/
│   ├── data_preprocessing/
│       ├── __init__.py
│       └── cleaner.py          # Data cleaning scripts
├── environment.yml             # Conda environment dependencies
├── main.py                     # Main execution script for the project
└── README.md                   # Project documentation (this file)
```

## 📊 Data Sources

This project uses the following datasets from **Open Data Barcelona** to prepare and clean data related to tourism, noise levels, and points of interest:

- **Tourist Density Areas**  
  **Source**: [Intensitat d'Activitat Turística](https://opendata-ajuntament.barcelona.cat/data/ca/dataset/intensitat-activitat-turistica)  
  **Format**: GPKG  
  **Description**: Areas in Barcelona with high concentrations of tourism, including accommodations, leisure, and tourist attractions.

- **Cultural Points of Interest**  
  **Source**: [Punts d'Interès Turístic](https://opendata-ajuntament.barcelona.cat/data/ca/dataset/punts-informacio-turistica)  
  **Description**: Detailed data on key cultural and tourist points of interest in the city.

- **Environmental Noise Monitoring Data**  
  **Source**: [Xarxa Soroll](https://opendata-ajuntament.barcelona.cat/data/ca/dataset/xarxasoroll-equipsmonitor-dades)  
  **Description**: Noise monitoring data in Barcelona aimed at understanding environmental noise levels across the city.

- **Geographic Data for Noise Sensors**  
  **Source**: [Geographic Data for Noise Sensors](https://opendata-ajuntament.barcelona.cat/data/ca/dataset/xarxasoroll-equipsmonitor-instal)  
  **Description**: Geographic information for noise sensors that wasn't included in the initial noise monitoring data.

## 🚀 Data Processing Summary:

1. **Tourist Concentration Data**[] (`process_gpkg_data`): 
    - **Steps**:
        - Load the dataset from GeoPackage format.
        - Calculate centroids for each geometry and reproject them to WGS84.
        - Extract latitude and longitude from centroids.
        - **Create random dates** within 2019, thinking about a possible future application for our app because the available data was not sufficient for our idea.
    - **Final Features**:
        - `lon`: Longitude of tourist concentration areas.
        - `lat`: Latitude of tourist concentration areas.
        - `date`: Random dates within 2019.
        - `day_of_week`: Day of the week for each date.
        - `month_name`: Month for each date.

2. **Tourist Sites Data** (`process_sites_data`):
    - **Steps**:
        - Load CSV data and clean the address by combining road name and street number, removing special characters.
        - Extract categories based on specific keywords (e.g., 'Centre', 'Museu') and highlight featured categories (e.g., 'Sagrada Família', 'Montjuïc').
    - **Final Features**:
        - `name`: Cleaned names of tourist sites.
        - `category`: Extracted categories of tourist places (e.g., 'destacat' for highlighted sites).
        - `address`: Cleaned and combined address fields.
        - `lat`: Latitude of tourist sites.
        - `lon`: Longitude of tourist sites.

3. **Environmental Noise Data** (`process_sound_data`):
    - **Steps**:
        - Load and clean sound level data, calculate mean sound levels per day.
        - Merge sound data with location data of sound sensors.
        - Drop missing values and unnecessary columns.
    - **Final Features**:
        - `lat`: Latitude of sound sensors.
        - `lon`: Longitude of sound sensors.
        - `date`: Date of the sound level measurements.
        - `sound_level_mean`: Daily mean sound level (LAeq) rounded to 2 decimals.

### Final Outputs:
- Three clean datasets:
    - **Tourist Concentration** data with centroids and random 2019 dates.
    - **Tourist Sites** data with categories and locations.
    - **Environmental Noise** data with daily average noise levels and sensor locations.
