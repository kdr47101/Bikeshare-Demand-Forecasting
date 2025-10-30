import pandas as pd
import requests
from pathlib import Path

def build_station_hour_data():
    """
    Fetch GBFS station_information and station_status data and create station availability dataset.
    """
    # Define paths
    script_dir = Path(__file__).parent
    output_path = script_dir / '..' / 'data' / 'interim'
    output_path.mkdir(parents=True, exist_ok=True)
    
    # GBFS Auto-discovery URL - REPLACE THIS WITH YOUR SYSTEM'S URL
    gbfs_url = "https://tor.publicbikesystem.net/ube/gbfs/v1/gbfs.json"
    
    print(f"Fetching GBFS auto-discovery from {gbfs_url}")
    
    try:
        # Get auto-discovery file
        response = requests.get(gbfs_url)
        response.raise_for_status()
        gbfs_data = response.json()
        
        # Extract feed URLs
        feeds = {feed['name']: feed['url'] for feed in gbfs_data['data']['en']['feeds']}
        
        # Fetch station_information (static data: name, capacity, location)
        print("Fetching station_information...")
        station_info_response = requests.get(feeds['station_information'])
        station_info_response.raise_for_status()
        station_info = station_info_response.json()['data']['stations']
        
        # Create DataFrame with station information
        stations_df = pd.DataFrame(station_info)
        
        # Select relevant columns
        # Note: 'capacity' field contains total docking capacity
        columns_to_keep = ['station_id', 'name', 'capacity', 'lat', 'lon']
        stations_df = stations_df[columns_to_keep]
        
        # Rename for clarity
        stations_df.columns = ['station_id', 'station_name', 'station_capacity', 'latitude', 'longitude']
        
        # Export to CSV
        output_file = output_path / 'station_data.csv'
        stations_df.to_csv(output_file, index=False)
        
        print(f"Successfully created {output_file}")
        print(f"Total stations: {len(stations_df)}")
        print(f"\nFirst few rows:")
        print(stations_df.head())
        
        # Check if capacity changes over time by fetching station_status
        print("\n" + "="*50)
        print("Checking if capacity changes over time...")
        station_status_response = requests.get(feeds['station_status'])
        station_status_response.raise_for_status()
        station_status = station_status_response.json()['data']['stations']
        
        # Check if 'num_docks_available' varies from static capacity
        status_df = pd.DataFrame(station_status)
        
        print(f"\nstation_status fields: {status_df.columns.tolist()}")
        print("\n⚠️  IMPORTANT NOTE:")
        print("The 'capacity' field in station_information is STATIC (doesn't change).")
        print("For real-time availability, you need station_status which has:")
        print("  - num_bikes_available: bikes currently at station")
        print("  - num_docks_available: empty docks at station")
        print("  - capacity = num_bikes_available + num_docks_available")
        
        return stations_df
        
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    build_station_hour_data()