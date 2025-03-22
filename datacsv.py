import os
import random
from datetime import datetime
import pandas as pd


def save_to_csv_file(data, filename):
    """
    Save data to a CSV file
    
    Args:
        data (dict): The data to save
        filename (str): The filename to save to
    """
    try:
        data.to_csv(filename, index=False)
        print(f"Data successfully saved to {filename}")
    
    except Exception as e:
        print(f"Error saving data: {e}")

def load_csv_file(filename):
    """
    Load data from a CSV file
    
    Args:
        filename (str): The filename to load from
        
    Returns:
        dict: The loaded CSV data
    """
    try:
        if not os.path.exists(filename):
            print(f"File {filename} does not exist")
            return None
            
        with open(filename, 'r') as f:
            data = pd.read_csv(f)
        print(f"Data successfully loaded from {filename}")
        return data
    
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def filter_data_by_year_range(data, start_year=2013, end_year=2019, year_column="year"):
    """
    Filter data entries to keep only those between start_year and end_year
    
    Args:
        data (list): List of data entries (dictionaries)
        start_year (int): Start year (inclusive)
        end_year (int): End year (inclusive)
        year_column (str): Name of the year column
        
    Returns:
        list: Filtered data entries
    """
    filtered_data = []
            
    try:
        # Ensure year column is numeric
        data[year_column] = pd.to_numeric(data[year_column], errors='coerce')

        # Drop rows with invalid or missing years
        data = data.dropna(subset=[year_column])

        # Filter rows within range
        mask = data[year_column].between(start_year, end_year)
        filtered_data = data[mask]

        print(f"Filtered data from {len(data)} to {len(filtered_data)} entries")
        return filtered_data
    
    except Exception as e:
        print(f"Error filtering data by year: {e}")
        return data

def random_sample_data(data, sample_size=10000, seed=1):
    """
    Randomly sample a fixed number of entries from the data
    
    Args:
        data (list): List of data entries (dictionaries)
        sample_size (int): Number of entries to sample
        seed (int): Random seed for reproducibility
        
    Returns:
        list: Sampled data entries
    """
    # Set random seed for reproducibility
    random.seed(seed)
    
    # If data has fewer entries than sample_size, return all data
    if len(data) <= sample_size:
        print(f"Data only has {len(data)} entries, returning all entries")
        return data
    
    # Randomly sample entries
    sampled_data = data.sample(n=sample_size, random_state=seed)
    print(f"Randomly sampled {sample_size} entries from {len(data)} entries")
    
    return sampled_data

# Example usage
if __name__ == "__main__":
    # Get the current directory where this script is located
    #current_dir = os.path.dirname(os.path.abspath(__file__))
    current_dir = os.getcwd() # Running in jupyter cell
    
    # Set output file paths in the current directory
    output_file = os.path.join(current_dir, "Data set 1.csv")
    filtered_file = os.path.join(current_dir, "filtered_data_2013_2019.csv")
    sampled_file = os.path.join(current_dir, "sampled_data_10k.csv")
    final_output_file = os.path.join(current_dir, "final_processed_data.csv")

    csv_data = load_csv_file(output_file)
    
    if csv_data is not None and not csv_data.empty:
        # Filter data to entries between 2013 and 2019
        filtered_data = filter_data_by_year_range(csv_data, 2013, 2019, "year")
        
        # Save filtered data
        if filtered_data is not None and not filtered_data.empty:
            save_to_csv_file(filtered_data, filtered_file)
            
            # Sample 10,000 random entries with fixed seed
            sampled_data = random_sample_data(filtered_data, sample_size=10000, seed=1)
            
            # Save sampled data
            if sampled_data is not None and not sampled_data.empty:
                save_to_csv_file(sampled_data, sampled_file)
                
                # Create a copy of the final sampled data as the final output
                save_to_csv_file(sampled_data, final_output_file)
                print(f"Final processed data saved to {final_output_file}")