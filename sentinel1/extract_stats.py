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

def log_info(message: str):
    """Log an informational message with timestamp."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[INFO]  {ts} Step: {message}")

def log_error(message: str):
    """Log an error message with timestamp."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[ERROR] {ts} Step: {message}", file=sys.stderr)

def extract_backscatter_stats(data_vv: np.ndarray, data_vh: np.ndarray, scene_id: str, field_id: str) -> dict:
    """Extract statistics from backscatter data (VV/VH)."""
    mean_vv = float(np.mean(data_vv))
    mean_vh = float(np.mean(data_vh))

    # Error condition if either mean is zero
    if mean_vv == 0.0 or mean_vh == 0.0:
        log_error(
            f"Scene {scene_id}, field {field_id}: mean_VV = {mean_vv}, mean_VH = {mean_vh}. "
            f"One or both values are 0 — possible edge case."
        )
        return None

    return {
        "scene_id": scene_id,
        "field_id": field_id,
        "mean_VV": mean_vv,
        "variance_VV": float(np.var(data_vv)),
        "min_VV": float(np.min(data_vv)),
        "max_VV": float(np.max(data_vv)),
        "mean_VH": mean_vh,
        "variance_VH": float(np.var(data_vh)),
        "min_VH": float(np.min(data_vh)),
        "max_VH": float(np.max(data_vh))
    }

def extract_poldecomp_stats(data_entropy: np.ndarray, data_alpha: np.ndarray, scene_id: str, field_id: str) -> dict:
    """Extract statistics from polarimetric decomposition data (entropy/alpha)."""
    mean_entropy = float(np.mean(data_entropy))
    mean_alpha = float(np.mean(data_alpha))

    # Error condition if either mean is zero
    if mean_entropy == 0.0 or mean_alpha == 0.0:
        log_error(
            f"Scene {scene_id}, field {field_id}: mean_entropy = {mean_entropy}, mean_alpha = {mean_alpha}. "
            f"One or both values are 0 — possible edge case."
        )
        return None

    return {
        "mean_entropy": mean_entropy,
        "variance_entropy": float(np.var(data_entropy)),
        "min_entropy": float(np.min(data_entropy)),
        "max_entropy": float(np.max(data_entropy)),
        "mean_alpha": mean_alpha,
        "variance_alpha": float(np.var(data_alpha)),
        "min_alpha": float(np.min(data_alpha)),
        "max_alpha": float(np.max(data_alpha)),
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
            band1 = src.read(1)
            band2 = src.read(2)

            log_info(f"Input file: {input_path}")
            if is_poldecomp:
                log_info(f"Entropy range: {np.min(band1)} to {np.max(band1)}")
                log_info(f"Alpha range: {np.min(band2)} to {np.max(band2)}")
            else:  # backscatter
                log_info(f"VV range: {np.min(band1)} to {np.max(band1)}")
                log_info(f"VH range: {np.min(band2)} to {np.max(band2)}")

            nodata = src.nodata
            if nodata is not None:
                log_info(f"Nodata value: {nodata}")
                band1 = band1[band1 != nodata]
                band2 = band2[band2 != nodata]
            else:
                log_info("No nodata value specified")
                band1 = band1[np.isfinite(band1)]
                band2 = band2[np.isfinite(band2)]

            log_info("computing_statistics")
            
            # Extract statistics based on file type
            if is_poldecomp:
                log_info("Processing polarimetric decomposition data (entropy/alpha)")
                stats = extract_poldecomp_stats(band1, band2, scene_id, field_id)
                if stats is None:
                    return None
                # Add common fields for poldecomp
                stats.update({
                    "acquisition_time": pd.to_datetime(acquisition_time),
                    "data_type": "poldecomp"
                })
            else:  # backscatter
                log_info("Processing backscatter data (VV/VH)")
                stats = extract_backscatter_stats(band1, band2, scene_id, field_id)
                if stats is None:
                    return None
                # Add acquisition time for backscatter
                stats["acquisition_time"] = pd.to_datetime(acquisition_time)

            log_info(f"Statistics computed successfully for {scene_id}")
            return stats

    except Exception as e:
        log_error(f"Failed to extract statistics: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Extract statistics from subset raster")
    parser.add_argument("input_path", help="Path to input raster file")
    parser.add_argument("scene_id", help="Scene identifier")
    parser.add_argument("field_id", help="Field identifier")
    parser.add_argument("acquisition_time", help="Acquisition time in ISO format")
    parser.add_argument("output_csv", help="Base path for output CSV file (without extension)")
    parser.add_argument("--log", help="Path to log file", default=None)
    args = parser.parse_args()

    try:
        if args.log:
            log_file = open(args.log, 'a')
            sys.stdout = log_file
            sys.stderr = log_file

        # Extract statistics
        stats = extract_stats(args.input_path, args.scene_id, args.field_id, args.acquisition_time)
        if stats is None:
            sys.exit(1)

        # Convert to DataFrame
        df = pd.DataFrame([stats])

        # Determine output CSV path based on data type
        input_filename = Path(args.input_path).name
        is_poldecomp = '_poldecomp_' in input_filename
        
        # Ensure output_csv is a file path, not a directory
        output_base = Path(args.output_csv)
        if output_base.is_dir():
            output_base = output_base / "stats_s1"
        
        if is_poldecomp:
            output_csv = str(output_base) + "_poldecomp.csv"
            log_info(f"Saving polarimetric decomposition statistics to: {output_csv}")
        else:  # backscatter
            output_csv = str(output_base) + "_backscatter.csv"
            log_info(f"Saving backscatter statistics to: {output_csv}")
        
        # Create parent directories if they don't exist
        Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
        
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