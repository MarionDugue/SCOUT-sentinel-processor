#!/usr/bin/env python3

"""
Subset dual-band TIFFs by KML polygons.
"""

import os
import sys
import argparse
import traceback
from pathlib import Path
from datetime import datetime

import rasterio
from rasterio.mask import mask
import geopandas as gpd
from shapely.geometry import mapping

def log_info(message: str, log_file=None):
    """Log an informational message with timestamp."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = f"[INFO]  {ts} Step: {message}"
    print(msg)
    if log_file:
        log_file.write(msg + '\n')
        log_file.flush()  # Ensure immediate write

def log_error(message: str, log_file=None):
    """Log an error message with timestamp."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = f"[ERROR] {ts} Step: {message}"
    print(msg, file=sys.stderr)
    if log_file:
        log_file.write(msg + '\n')
        log_file.flush()  # Ensure immediate write

def subset_raster(input_path: str, output_path: str, kml_path: str, log_file=None) -> bool:
    """
    Subset a raster file using a KML polygon.
    
    Args:
        input_path: Path to input raster file
        output_path: Path to save subset raster
        kml_path: Path to KML file containing polygon
        log_file: Optional file object for logging
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if not os.path.exists(input_path):
            log_error(f"Input file does not exist: {input_path}", log_file)
            return False

        if not os.path.exists(kml_path):
            log_error(f"KML file not found: {kml_path}", log_file)
            return False

        log_info(f"Opening source: {input_path}", log_file)
        with rasterio.open(input_path) as src:
            log_info(f"Source CRS: {src.crs}", log_file)
            log_info(f"Source transform: {src.transform}", log_file)
            log_info(f"Source shape: {src.shape}", log_file)
            
            log_info(f"Reading KML file: {kml_path}", log_file)
            try:
                gdf = gpd.read_file(kml_path, driver='KML')
            except Exception as e:
                log_error(f"Failed to read KML file: {str(e)}", log_file)
                return False

            if gdf.empty:
                log_error("KML file has no geometries.", log_file)
                return False

            log_info(f"KML CRS: {gdf.crs}", log_file)
            log_info(f"Number of geometries: {len(gdf)}", log_file)
            
            log_info(f"Transforming geometries to raster CRS ({src.crs})", log_file)
            try:
                gdf = gdf.to_crs(src.crs)
            except Exception as e:
                log_error(f"Failed to transform geometries: {str(e)}", log_file)
                return False

            geojson_polygons = [mapping(geom) for geom in gdf.geometry if geom is not None]
            if not geojson_polygons:
                log_error("No valid geometries found in KML.", log_file)
                return False

            log_info(f"Number of valid geometries: {len(geojson_polygons)}", log_file)
            
            log_info("Applying mask and cropping to footprint", log_file)
            try:
                subset_data, subset_transform = mask(src, geojson_polygons, crop=True)
            except Exception as e:
                log_error(f"Failed to apply mask: {str(e)}", log_file)
                return False

            if hasattr(subset_data, "mask") and subset_data.mask.all():
                log_error("Subset result is empty — no raster overlap with KML geometry.", log_file)
                return False

            log_info(f"Subset shape: {subset_data.shape}", log_file)
            log_info(f"Subset transform: {subset_transform}", log_file)

            out_meta = src.meta.copy()
            out_meta.update({
                "driver": "GTiff",
                "height": subset_data.shape[1],
                "width": subset_data.shape[2],
                "transform": subset_transform
            })

            log_info(f"Writing output to: {output_path}", log_file)
            try:
                with rasterio.open(output_path, "w", **out_meta) as dst:
                    dst.write(subset_data)
            except Exception as e:
                log_error(f"Failed to write output: {str(e)}", log_file)
                return False

            if os.path.exists(output_path):
                log_info(f"Subset saved successfully: {output_path}", log_file)
                return True
            else:
                log_error(f"Output file not written: {output_path}", log_file)
                return False

    except Exception as e:
        log_error(f"Unexpected error in subset_raster: {str(e)}", log_file)
        log_error(f"Traceback: {traceback.format_exc()}", log_file)
        return False

def main():
    parser = argparse.ArgumentParser(description="Subset raster by KML polygon")
    parser.add_argument("--config", required=True, help="Path to config file")
    parser.add_argument("--input", required=True, help="Path to input raster")
    parser.add_argument("--kml", required=True, help="Path to KML file")
    parser.add_argument("--output", required=True, help="Path to output subset")
    parser.add_argument("--log", help="Path to log file")
    
    args = parser.parse_args()
    
    # Set up logging if log file specified
    log_file = None
    if args.log:
        try:
            # Ensure log directory exists
            os.makedirs(os.path.dirname(args.log), exist_ok=True)
            log_file = open(args.log, 'a')
            log_info(f"Starting subsetting process", log_file)
            log_info(f"Input: {args.input}", log_file)
            log_info(f"KML: {args.kml}", log_file)
            log_info(f"Output: {args.output}", log_file)
        except Exception as e:
            print(f"Failed to set up logging: {str(e)}", file=sys.stderr)
            sys.exit(1)
    
    try:
        success = subset_raster(args.input, args.output, args.kml, log_file)
        if log_file:
            log_info("Subsetting process completed", log_file)
        sys.exit(0 if success else 1)
    except Exception as e:
        if log_file:
            log_error(f"Unexpected error in main: {str(e)}", log_file)
            log_error(f"Traceback: {traceback.format_exc()}", log_file)
        else:
            print(f"Unexpected error: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        if log_file:
            log_file.close()

if __name__ == "__main__":
    main() 