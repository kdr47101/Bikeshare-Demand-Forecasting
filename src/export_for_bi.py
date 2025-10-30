<<<<<<< HEAD
﻿import pandas as pd
import os
from pathlib import Path

def process_ridership_data():
    """
    Process monthly ridership data to create BI summary with total trips and unique stations.
    """
    # Define paths
    interim_path = Path(__file__).parent.parent / "data" / "interim"
    output_path = Path(__file__).parent.parent / "data" / "processed"
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    total_trips = 0
    unique_stations = set()
    
    # Iterate through all subdirectories in interim folder
    for folder in interim_path.iterdir():
        if folder.is_dir():
            print(f"Processing folder: {folder.name}")
            
            # Process all CSV files in the folder
            for csv_file in folder.glob("*.csv"):
                print(f"  Reading: {csv_file.name}")
                
                try:
                    # Try multiple encodings
                    df = None
                    for encoding in ['utf-8', 'latin-1', 'windows-1252', 'cp1252']:
                        try:
                            df = pd.read_csv(csv_file, encoding=encoding)
                            break
                        except UnicodeDecodeError:
                            continue
                    
                    if df is None:
                        print(f"  Error: Could not decode {csv_file.name}")
                        continue
                    
                    # Count trips
                    total_trips += len(df)
                    
                    # Handle different column naming conventions
                    # Old format: from_station_id, to_station_id
                    # New format: Start Station Id, End Station Id
                    if 'from_station_id' in df.columns:
                        unique_stations.update(df['from_station_id'].dropna().unique())
                        unique_stations.update(df['to_station_id'].dropna().unique())
                    elif 'Start Station Id' in df.columns:
                        unique_stations.update(df['Start Station Id'].dropna().unique())
                        unique_stations.update(df['End Station Id'].dropna().unique())
                    else:
                        print(f"  Warning: Unknown column format in {csv_file.name}")
                    
                except Exception as e:
                    print(f"  Error processing {csv_file.name}: {e}")
    
    # Create summary dataframe
    summary_df = pd.DataFrame({
        'Total_trips': [total_trips],
        'Num_unique_stations': [len(unique_stations)]
    })
    
    # Save to CSV
    output_file = output_path / "ridership_summary.csv"
    summary_df.to_csv(output_file, index=False)
    
    print(f"\n{'='*50}")
    print(f"Summary Report")
    print(f"{'='*50}")
    print(f"Total Trips: {total_trips:,}")
    print(f"Unique Stations: {len(unique_stations):,}")
    print(f"\nOutput saved to: {output_file}")
    
    return summary_df

if __name__ == "__main__":
    process_ridership_data()