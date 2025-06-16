#!/usr/bin/env python3

"""
Subset raster files by multiple KML polygons.
For each input scene, creates subsets for each KML in the specified directory.
All parameters are read from the config file.
"""

import os
import sys
import argparse
import traceback
import yaml
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
        log_file.flush()

def log_error(message: str, log_file=None):
    """Log an error message with timestamp."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = f"[ERROR] {ts} Step: {message}"
    print(msg, file=sys.stderr)
    if log_file:
        log_file.write(msg + '\n')
        log_file.flush()

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

            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

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

def process_scene_with_kmls(input_file: str, kml_dir: str, output_dir: str, log_file: str) -> None:
    """
    Process a single scene with all KMLs in the directory.
    
    Args:
        input_file: Path to input TIF file
        kml_dir: Directory containing KML files
        output_dir: Base output directory (will create subdirectories for each KML)
        log_file: Path to log file
    """
    # Get the complete scene name from the input file path
    scene_name = os.path.basename(input_file).replace('.tif', '')
    
    # Get list of KML files
    kml_files = [f for f in os.listdir(kml_dir) if f.endswith('.kml')]
    if not kml_files:
        log_error(f"No KML files found in {kml_dir}", log_file)
        return

    # Process each KML file
    for kml_file in kml_files:
        kml_name = os.path.splitext(kml_file)[0]  # Get KML name without extension
        kml_path = os.path.join(kml_dir, kml_file)
        
        # Create output directory for this KML
        kml_output_dir = os.path.join(output_dir, kml_name)
        os.makedirs(kml_output_dir, exist_ok=True)
        
        # Set output file name to include both complete scene name and field name
        output_file = os.path.join(kml_output_dir, f"{scene_name}_{kml_name}.tif")
        
        log_info(f"Processing KML: {kml_name}")
        log_info(f"Output will be saved to: {output_file}")
        
        try:
            # Process the subset
            subset_raster(input_file, output_file, kml_path, log_file)
            log_info(f"Successfully created subset for {kml_name}")
        except Exception as e:
            log_error(f"Failed to process subset for {kml_name}: {str(e)}", log_file)
            continue

def get_config_paths(config: dict, base_dir: str) -> dict:
    """
    Get all necessary paths from config file.
    
    Args:
        config: Loaded config dictionary
        base_dir: Base directory for relative paths
        
    Returns:
        dict: Dictionary containing all necessary paths
    """
    try:
        # Get input paths - use absolute path from config
        download_dir = config['sentinel1']['download_dir']  # Use absolute path directly
        final_dir = os.path.join(download_dir, config['sentinel1']['snap']['output_dir'].replace('{download_dir}', download_dir))
        
        # Use absolute path for aoi_path from config
        aoi_path = config['input']['aoi_path']  # Use absolute path directly
        
        # Set output directory to be under download_dir/final/subset
        subset_dir = os.path.join(download_dir, 'final', 'subsets')
        
        return {
            'input_dir': final_dir,  # Use the final directory from SNAP processing
            'aoi_path': aoi_path,
            'output_dir': subset_dir,  # This will be used as base directory for kml_field subdirectories
        }
    except KeyError as e:
        raise ValueError(f"Missing required config parameter: {e}")

def main():
    """Main function to process input files with KMLs."""
    parser = argparse.ArgumentParser(description='Subset raster files using KML files')
    parser.add_argument('--config', required=True, help='Path to config file')
    parser.add_argument('--log', help='Path to log file')
    args = parser.parse_args()

    # Setup logging
    log_file = None
    if args.log:
        try:
            os.makedirs(os.path.dirname(args.log), exist_ok=True)
            log_file = open(args.log, 'a')
            log_info("Starting subsetting process", log_file)
        except Exception as e:
            print(f"Failed to set up logging: {str(e)}", file=sys.stderr)
            sys.exit(1)
    
    try:
        # Load config
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(args.config)))
        
        # Get paths from config
        paths = get_config_paths(config, base_dir)
        
        # Process input directory
        input_path = Path(paths['input_dir'])
        if not input_path.exists():
            log_error(f"Input path does not exist: {input_path}", log_file)
            sys.exit(1)
            
        if input_path.is_file():
            # Single file processing
            process_scene_with_kmls(str(input_path), paths['aoi_path'], paths['output_dir'], log_file)
        else:
            # Directory processing
            tif_files = sorted(input_path.glob("*.tif"))
            if not tif_files:
                log_error(f"No TIFF files found in {input_path}", log_file)
                sys.exit(1)
                
            log_info(f"Found {len(tif_files)} TIFF files to process", log_file)
            for tif_file in tif_files:
                process_scene_with_kmls(str(tif_file), paths['aoi_path'], paths['output_dir'], log_file)
        
        if log_file:
            log_info("Subsetting process completed", log_file)
        sys.exit(0)
        
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