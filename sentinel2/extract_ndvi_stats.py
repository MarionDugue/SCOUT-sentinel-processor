#!/usr/bin/env python3

"""
Extract NDVI statistics from Sentinel-2 GeoTIFF files.
Extracts mean, variance, min, max NDVI values for each scene and saves to CSV.
"""

import sys
import argparse
from datetime import datetime
import pandas as pd
import numpy as np
import rasterio
from pathlib import Path
import os
import yaml
import re

def log_info(message: str):
    """Log an informational message with timestamp."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[INFO]  {ts} Step: {message}")

def log_error(message: str):
    """Log an error message with timestamp."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[ERROR] {ts} Step: {message}", file=sys.stderr)

def extract_ndvi_stats(data: np.ndarray, scene_name: str, acquisition_date: str, field_id: str = None) -> dict:
    """Extract NDVI statistics from data array."""
    # Remove invalid values (NaN, inf, nodata)
    valid_data = data[np.isfinite(data)]
    
    if len(valid_data) == 0:
        log_error(f"Scene {scene_name}: No valid NDVI data found")
        return None
    
    # Calculate statistics
    ndvi_mean = float(np.mean(valid_data))
    ndvi_variance = float(np.var(valid_data))
    ndvi_min = float(np.min(valid_data))
    ndvi_max = float(np.max(valid_data))
    
    # Check for reasonable NDVI values (should be between -1 and 1)
    if ndvi_min < -1.0 or ndvi_max > 1.0:
        log_error(f"Scene {scene_name}: NDVI values out of expected range [-1, 1]: min={ndvi_min}, max={ndvi_max}")
        return None
    
    stats = {
        "ndvi_mean": ndvi_mean,
        "ndvi_variance": ndvi_variance,
        "ndvi_min": ndvi_min,
        "ndvi_max": ndvi_max,
        "scene_name": scene_name,
        "acquisition_date": pd.to_datetime(acquisition_date)
    }
    
    # Add field_id if provided
    if field_id is not None:
        stats["field_id"] = field_id
    
    return stats

def extract_stats_from_tiff(input_path: str, scene_name: str, acquisition_date: str, field_id: str = None) -> dict:
    """
    Extract NDVI statistics from a GeoTIFF file.
    
    Args:
        input_path: Path to input GeoTIFF file
        scene_name: Name of the scene
        acquisition_date: Acquisition date in ISO format
        field_id: Field identifier (optional)
        
    Returns:
        dict: Dictionary containing NDVI statistics
    """
    try:
        with rasterio.open(input_path) as src:
            log_info(f"Reading NDVI data from: {input_path}")
            
            # Read the NDVI band (should be the first band)
            data = src.read(1)
            
            log_info(f"Data shape: {data.shape}")
            log_info(f"Data range: {np.min(data)} to {np.max(data)}")
            
            # Get nodata value
            nodata = src.nodata
            if nodata is not None:
                log_info(f"Nodata value: {nodata}")
                data = data[data != nodata]
            else:
                log_info("No nodata value specified")
                data = data[np.isfinite(data)]
            
            log_info("Computing NDVI statistics")
            
            stats = extract_ndvi_stats(data, scene_name, acquisition_date, field_id)
            if stats is None:
                return None
            
            log_info(f"Statistics computed successfully for {scene_name}")
            return stats

    except Exception as e:
        log_error(f"Failed to extract statistics from {input_path}: {e}")
        return None

def extract_ndvi_stats_from_directory(input_dir: str, config: dict, field_id: str = None) -> pd.DataFrame:
    """
    Extract NDVI statistics from all GeoTIFF files in a directory.
    
    Args:
        input_dir: Directory containing GeoTIFF files
        config: Configuration dictionary
        field_id: Field identifier (optional)
        
    Returns:
        pd.DataFrame: DataFrame containing all NDVI statistics
    """
    all_stats = []
    
    # Find all GeoTIFF files in the directory
    tiff_files = list(Path(input_dir).glob("*.tif"))
    
    if not tiff_files:
        log_error(f"No GeoTIFF files found in {input_dir}")
        return pd.DataFrame()
    
    log_info(f"Found {len(tiff_files)} GeoTIFF files to process")
    
    for tiff_file in tiff_files:
        # Extract scene name and date from filename
        # Expected format: NDVI_YYYY-MM-DD_aoi_name.tif
        filename = tiff_file.stem
        
        # Parse filename to extract date and scene name
        match = re.match(r"NDVI_(\d{4}-\d{2}-\d{2})_(.+)", filename)
        if match:
            acquisition_date = match.group(1)
            scene_name = match.group(2)
        else:
            log_error(f"Could not parse filename: {filename}")
            continue
        
        log_info(f"Processing: {filename}")
        
        stats = extract_stats_from_tiff(str(tiff_file), scene_name, acquisition_date, field_id)
        if stats is not None:
            all_stats.append(stats)
    
    return pd.DataFrame(all_stats)

def extract_ndvi_stats_from_directory_with_field_id(input_dir: str, config: dict, field_id: str) -> pd.DataFrame:
    """
    Extract NDVI statistics from all GeoTIFF files in a directory with field_ID tracking.
    
    Args:
        input_dir: Directory containing GeoTIFF files
        config: Configuration dictionary
        field_id: Field identifier
        
    Returns:
        pd.DataFrame: DataFrame containing all NDVI statistics with field_ID column
    """
    return extract_ndvi_stats_from_directory(input_dir, config, field_id)

def main():
    parser = argparse.ArgumentParser(description="Extract NDVI statistics from Sentinel-2 GeoTIFF files")
    parser.add_argument("--config", required=True, help="Path to config file")
    parser.add_argument("--input_dir", required=True, help="Directory containing GeoTIFF files")
    parser.add_argument("--output_csv", required=True, help="Base path for output CSV file (without extension)")
    parser.add_argument("--field_id", help="Field identifier to include in statistics")
    parser.add_argument("--log", help="Path to log file")
    
    args = parser.parse_args()
    
    # Load config file
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Set up logging if log file specified
    if args.log:
        log_file = open(args.log, 'a')
        sys.stdout = log_file
        sys.stderr = log_file
    
    try:
        # Extract statistics from all GeoTIFF files
        df = extract_ndvi_stats_from_directory(args.input_dir, config, args.field_id)
        
        if df.empty:
            log_error("No valid statistics extracted")
            sys.exit(1)
        
        # Get config variables for filename
        s2_config = config.get('sentinel2', {})
        start_date = config['input']['start_date'].replace('-', '')
        end_date = config['input']['end_date'].replace('-', '')
        
        # Get output directory from config
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(args.config))), config['output']['base_dir'])
        os.makedirs(output_dir, exist_ok=True)
        
        # Create descriptive filename following the same pattern as Sentinel-1
        output_csv = os.path.join(output_dir, f"stats_ndvi_S2_{start_date}_{end_date}.csv")
        log_info(f"Saving NDVI statistics to: {output_csv}")
        
        # Append to existing CSV if it exists, otherwise create new
        if os.path.exists(output_csv):
            df.to_csv(output_csv, mode='a', header=False, index=False)
        else:
            df.to_csv(output_csv, index=False)
        
        log_info(f"Successfully processed {len(df)} scenes")
        
    except Exception as e:
        log_error(f"Failed to process NDVI statistics: {e}")
        sys.exit(1)
    finally:
        if args.log:
            log_file.close()

if __name__ == "__main__":
    main() 