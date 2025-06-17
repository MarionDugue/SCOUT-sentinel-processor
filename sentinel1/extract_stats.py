#!/usr/bin/env python3

"""
Extract statistics from subset raster files.
Handles both backscatter (dB) and polarimetric decomposition (entropy/alpha) statistics.
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

def extract_orbit_info(scene_id: str, acquisition_time: str) -> tuple:
    """
    Extract orbit direction and relative orbit from scene ID and acquisition time.
    
    Args:
        scene_id: Sentinel-1 scene identifier
        acquisition_time: Acquisition time in ISO format
        
    Returns:
        tuple: (orbit_direction, relative_orbit)
    """
    try:
        # Parse acquisition time to get hour
        acq_datetime = pd.to_datetime(acquisition_time)
        hour = acq_datetime.hour
        
        # Determine orbit direction based on hour
        # Based on the reference code: if hour is 5, it's Descending, otherwise Ascending
        orbit_direction = 'Descending' if hour == 5 else 'Ascending'
        
        # Extract absolute orbit number from scene ID
        # Sentinel-1 scene ID format: S1A_IW_SLC__1SDV_20240101T060000_20240101T060030_051234_062345_1234.SAFE
        # The absolute orbit number is typically in the 6th position after splitting by '_'
        scene_parts = scene_id.split('_')
        
        if len(scene_parts) >= 6:
            # Try to extract absolute orbit from the 6th position
            absolute_orbit_str = scene_parts[5]
            
            # Clean the string to get only digits
            absolute_orbit_match = re.search(r'(\d+)', absolute_orbit_str)
            if absolute_orbit_match:
                absolute_orbit = int(absolute_orbit_match.group(1))
                
                # Determine satellite from scene ID
                satellite = scene_parts[0]  # S1A or S1B
                
                # Calculate relative orbit based on satellite
                if satellite == 'S1A':
                    relative_orbit = ((absolute_orbit - 73) % 175) + 1
                elif satellite == 'S1B':
                    relative_orbit = ((absolute_orbit - 27) % 175) + 1
                else:
                    log_error(f"Unknown satellite in scene ID: {satellite}")
                    relative_orbit = None
            else:
                log_error(f"Could not extract absolute orbit from scene ID: {scene_id}")
                relative_orbit = None
        else:
            log_error(f"Scene ID format not recognized: {scene_id}")
            relative_orbit = None
        
        return orbit_direction, relative_orbit
        
    except Exception as e:
        log_error(f"Failed to extract orbit info from scene {scene_id}: {e}")
        return None, None

def extract_backscatter_stats(data_vv: np.ndarray, data_vh: np.ndarray, data_epsia: np.ndarray, scene_id: str, field_id: str) -> dict:
    """Extract statistics from backscatter data (VV/VH/epsIA)."""
    mean_vv = float(np.mean(data_vv))
    mean_vh = float(np.mean(data_vh))
    mean_epsia = float(np.mean(data_epsia))

    # Error condition if any mean is zero
    if mean_vv == 0.0 or mean_vh == 0.0 or mean_epsia == 0.0:
        log_error(
            f"Scene {scene_id}, field {field_id}: mean_VV = {mean_vv}, mean_VH = {mean_vh}, mean_epsIA = {mean_epsia}. "
            f"One or more values are 0 — possible edge case."
        )
        return None

    return {
        "mean_VV": mean_vv,
        "variance_VV": float(np.var(data_vv)),
        "min_VV": float(np.min(data_vv)),
        "max_VV": float(np.max(data_vv)),
        "mean_VH": mean_vh,
        "variance_VH": float(np.var(data_vh)),
        "min_VH": float(np.min(data_vh)),
        "max_VH": float(np.max(data_vh)),
        "mean_epsIA": mean_epsia,
        "variance_epsIA": float(np.var(data_epsia)),
        "min_epsIA": float(np.min(data_epsia)),
        "max_epsIA": float(np.max(data_epsia)),
    }

def extract_poldecomp_stats(data_entropy: np.ndarray, data_anisotropy: np.ndarray, data_alpha: np.ndarray, data_epsia: np.ndarray, scene_id: str, field_id: str) -> dict:
    """Extract statistics from polarimetric decomposition data (entropy/anisotropy/alpha/epsIA)."""
    mean_entropy = float(np.mean(data_entropy))
    mean_anisotropy = float(np.mean(data_anisotropy))
    mean_alpha = float(np.mean(data_alpha))
    mean_epsia = float(np.mean(data_epsia))

    # Error condition if any mean is zero
    if mean_entropy == 0.0 or mean_anisotropy == 0.0 or mean_alpha == 0.0 or mean_epsia == 0.0:
        log_error(
            f"Scene {scene_id}, field {field_id}: mean_entropy = {mean_entropy}, mean_anisotropy = {mean_anisotropy}, "
            f"mean_alpha = {mean_alpha}, mean_epsIA = {mean_epsia}. One or more values are 0 — possible edge case."
        )
        return None

    return {
        "mean_entropy": mean_entropy,
        "variance_entropy": float(np.var(data_entropy)),
        "min_entropy": float(np.min(data_entropy)),
        "max_entropy": float(np.max(data_entropy)),
        "mean_anisotropy": mean_anisotropy,
        "variance_anisotropy": float(np.var(data_anisotropy)),
        "min_anisotropy": float(np.min(data_anisotropy)),
        "max_anisotropy": float(np.max(data_anisotropy)),
        "mean_alpha": mean_alpha,
        "variance_alpha": float(np.var(data_alpha)),
        "min_alpha": float(np.min(data_alpha)),
        "max_alpha": float(np.max(data_alpha)),
        "mean_epsIA": mean_epsia,
        "variance_epsIA": float(np.var(data_epsia)),
        "min_epsIA": float(np.min(data_epsia)),
        "max_epsIA": float(np.max(data_epsia)),
    }

def extract_stats(input_path: str, scene_id: str, field_id: str, acquisition_time: str) -> dict:
    """
    Extract statistics from a raster file.
    Handles both backscatter (dB) and polarimetric decomposition (entropy/alpha) statistics.
    
    Args:
        input_path: Path to input raster file
        scene_id: Scene identifier
        field_id: Field identifier
        acquisition_time: Acquisition time in ISO format
        
    Returns:
        dict: Dictionary containing statistics
    """
    try:
        input_filename = Path(input_path).name
        is_poldecomp = '_poldecomp_' in input_filename
        is_dB = '_dB_' in input_filename

        with rasterio.open(input_path) as src:
            log_info("reading_bands")
            
            # Define band indices based on data type
            if is_poldecomp:
                # For poldecomp: bands 1,2,3,5 for entropy, anisotropy, alpha, epsIA
                band_indices = [1, 2, 3, 5]
                band_names = ['entropy', 'anisotropy', 'alpha', 'epsIA']
            else:  # backscatter
                # For backscatter: bands 1,2,4 for VV, VH, epsIA
                band_indices = [1, 2, 4]
                band_names = ['VV', 'VH', 'epsIA']
            
            # Read all required bands
            bands = [src.read(i) for i in band_indices]
            
            log_info(f"Input file: {input_path}")
            for band, name in zip(bands, band_names):
                log_info(f"{name} range: {np.min(band)} to {np.max(band)}")

            nodata = src.nodata
            if nodata is not None:
                log_info(f"Nodata value: {nodata}")
                bands = [band[band != nodata] for band in bands]
            else:
                log_info("No nodata value specified")
                bands = [band[np.isfinite(band)] for band in bands]

            log_info("computing_statistics")
            
            # Extract statistics based on file type
            if is_poldecomp:
                log_info("Processing polarimetric decomposition data (entropy/anisotropy/alpha/epsIA)")
                stats = extract_poldecomp_stats(bands[0], bands[1], bands[2], bands[3], scene_id, field_id)
                if stats is None:
                    return None
            else:  # backscatter
                log_info("Processing backscatter data (VV/VH/epsIA)")
                stats = extract_backscatter_stats(bands[0], bands[1], bands[2], scene_id, field_id)
                if stats is None:
                    return None

            # Extract orbit information
            log_info("extracting_orbit_information")
            orbit_direction, relative_orbit = extract_orbit_info(scene_id, acquisition_time)
            
            # Add common fields
            stats.update({
                "scene_id": scene_id,
                "field_id": field_id,
                "acquisition_time": pd.to_datetime(acquisition_time),
                "data_type": "poldecomp" if is_poldecomp else "backscatter",
                "orbit_direction": orbit_direction,
                "relative_orbit": relative_orbit
            })

            log_info(f"Statistics computed successfully for {scene_id}")
            return stats

    except Exception as e:
        log_error(f"Failed to extract statistics: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Extract statistics from subset raster")
    parser.add_argument("--config", required=True, help="Path to config file")
    parser.add_argument("--input", required=True, help="Path to input raster")
    parser.add_argument("--scene_id", required=True, help="Scene identifier")
    parser.add_argument("--field_id", required=True, help="Field identifier")
    parser.add_argument("--acquisition_time", required=True, help="Acquisition time (YYYY-MM-DDTHH:MM:SS)")
    parser.add_argument("--output_csv", required=True, help="Base path for output CSV files (without extension)")
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
        stats = extract_stats(args.input, args.scene_id, args.field_id, args.acquisition_time)
        if stats is None:
            sys.exit(1)
            
        df = pd.DataFrame([stats])
        data_type = stats["data_type"]
        
        # Get config variables for filename
        s1_config = config['sentinel1']
        start_date = config['input']['start_date'].replace('-', '')
        end_date = config['input']['end_date'].replace('-', '')
        rel_orbit = s1_config.get('rel_orbit', 'ALL')
        
        # Get output directory from config
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(args.config))), config['output']['base_dir'])
        os.makedirs(output_dir, exist_ok=True)
        
        # Create descriptive filename
        if data_type == "poldecomp":
            output_csv = os.path.join(output_dir, f"stats_poldecomp_S1_{s1_config['satellite']}_{s1_config['mode']}_{s1_config['level']}_{s1_config['polarisation'].replace('+', '')}_{start_date}_{end_date}_orbit{rel_orbit}.csv")
            log_info(f"Saving polarimetric decomposition statistics to: {output_csv}")
        else:  # backscatter
            output_csv = os.path.join(output_dir, f"stats_dB_S1_{s1_config['satellite']}_{s1_config['mode']}_{s1_config['level']}_{s1_config['polarisation'].replace('+', '')}_{start_date}_{end_date}_orbit{rel_orbit}.csv")
            log_info(f"Saving backscatter statistics to: {output_csv}")
        
        # Append to existing CSV if it exists, otherwise create new
        if os.path.exists(output_csv):
            df.to_csv(output_csv, mode='a', header=False, index=False)
        else:
            df.to_csv(output_csv, index=False)
        
    except Exception as e:
        log_error(f"Failed to process statistics: {e}")
        sys.exit(1)
    finally:
        if args.log:
            log_file.close()

if __name__ == "__main__":
    main() 