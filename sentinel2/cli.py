import os
import glob
import argparse
import logging
import sys
import yaml
import ndvi_exporter
import extract_ndvi_stats
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--extract_stats", action="store_true", help="Extract NDVI statistics from generated GeoTIFF files")
    parser.add_argument("--extract-existing-only", action="store_true", help="Extract NDVI statistics from existing GeoTIFF files only (skip export)")
    return parser.parse_args()


def make_absolute(path, base_dir):
    return (
        path if os.path.isabs(path) else os.path.abspath(os.path.join(base_dir, path))
    )


def extract_stats_from_existing_files(config_path, cfg, base_out_dir):
    """Extract NDVI statistics from existing GeoTIFF files in field subdirectories."""
    print("Extracting NDVI statistics from existing files...")
    
    # Find all field subdirectories in the data directory
    field_dirs = []
    for item in os.listdir(base_out_dir):
        item_path = os.path.join(base_out_dir, item)
        if os.path.isdir(item_path) and item.endswith('_NDVI'):
            field_dirs.append(item_path)
    
    if not field_dirs:
        print("No field NDVI directories found in data directory")
        return
    
    print(f"Found {len(field_dirs)} field directories to process")
    
    all_stats = []
    
    for field_dir in field_dirs:
        field_id = os.path.basename(field_dir).replace('_NDVI', '')
        print(f"Processing field: {field_id}")
        
        # Find all GeoTIFF files in this field directory
        tiff_files = glob.glob(os.path.join(field_dir, "*.tif"))
        
        if not tiff_files:
            print(f"No GeoTIFF files found in {field_dir}")
            continue
        
        print(f"Found {len(tiff_files)} GeoTIFF files in {field_id}")
        
        # Create a temporary config for the stats extraction
        stats_config = {
            'input': {'start_date': cfg['input']['start_date'], 'end_date': cfg['input']['end_date']},
            'output': {'base_dir': base_out_dir},
            'sentinel2': cfg['sentinel2']
        }
        
        # Extract statistics from this field directory
        df = extract_ndvi_stats.extract_ndvi_stats_from_directory_with_field_id(field_dir, stats_config, field_id)
        
        if not df.empty:
            all_stats.append(df)
            print(f"Extracted statistics for {len(df)} scenes from {field_id}")
        else:
            print(f"No valid statistics extracted from {field_id}")
    
    if all_stats:
        # Combine all statistics into one DataFrame
        combined_df = pd.concat(all_stats, ignore_index=True)
        
        # Save to CSV with the same naming pattern as Sentinel-1
        start_date_clean = cfg['input']['start_date'].replace('-', '')
        end_date_clean = cfg['input']['end_date'].replace('-', '')
        output_csv = os.path.join(base_out_dir, f"stats_ndvi_S2_{start_date_clean}_{end_date_clean}.csv")
        
        # Append to existing CSV if it exists, otherwise create new
        if os.path.exists(output_csv):
            combined_df.to_csv(output_csv, mode='a', header=False, index=False)
        else:
            combined_df.to_csv(output_csv, index=False)
        
        print(f"Saved combined NDVI statistics to {output_csv}")
        print(f"Total scenes processed: {len(combined_df)}")
    else:
        print("No statistics extracted from any field")


def main():
    logging.basicConfig(filename="ndvi_export.log", level=logging.INFO)

    args = parse_args()
    config_path = os.path.abspath(args.config)
    config_dir = os.path.dirname(config_path)

    # Load YAML config
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    # Resolve paths relative to config file
    aoi_path = make_absolute(cfg["input"]["aoi_path"], config_dir)
    base_out_dir = make_absolute(cfg["output"]["base_dir"], config_dir)
    start_date = cfg["input"]["start_date"]
    end_date = cfg["input"]["end_date"]

    cloud = cfg["sentinel2"]["cloud_threshold"]
    s2_sr_collection = cfg["sentinel2"]["surface_reflectance"]
    s2_cp_collection = cfg["sentinel2"]["cloud_probability"]

    # If only extracting from existing files, skip the export
    if args.extract_existing_only:
        extract_stats_from_existing_files(config_path, cfg, base_out_dir)
        return

    # Check if aoi_path is a directory or a file
    if os.path.isdir(aoi_path):
        kml_files = glob.glob(os.path.join(aoi_path, "*.kml"))
        if not kml_files:
            print(f"No .kml files found in directory {aoi_path}", file=sys.stderr)
            sys.exit(1)
    else:
        kml_files = [aoi_path]

    try:
        for kml_file in kml_files:
            basename = os.path.splitext(os.path.basename(kml_file))[0]
            out_dir = os.path.join(base_out_dir, f"{basename}_NDVI")
            os.makedirs(out_dir, exist_ok=True)

            print(f"Processing {kml_file} -> {out_dir}")
            ndvi_exporter.export_ndvi(
                aoi_path=kml_file,
                start_date=start_date,
                end_date=end_date,
                cloud_threshold=cloud,
                output_dir=out_dir,
                s2_sr_collection=s2_sr_collection,
                s2_cp_collection=s2_cp_collection,
            )

        logging.info("NDVI export completed successfully for all AOIs.")
        
        # Extract statistics if requested
        if args.extract_stats:
            print("Extracting NDVI statistics from generated GeoTIFF files...")
            for kml_file in kml_files:
                basename = os.path.splitext(os.path.basename(kml_file))[0]
                out_dir = os.path.join(base_out_dir, f"{basename}_NDVI")
                field_id = basename
                
                if os.path.exists(out_dir):
                    print(f"Extracting statistics from {out_dir}")
                    # Create a temporary config for the stats extraction
                    stats_config = {
                        'input': {'start_date': start_date, 'end_date': end_date},
                        'output': {'base_dir': base_out_dir},
                        'sentinel2': cfg['sentinel2']
                    }
                    
                    # Extract statistics from the directory
                    df = extract_ndvi_stats.extract_ndvi_stats_from_directory_with_field_id(out_dir, stats_config, field_id)
                    
                    if not df.empty:
                        # Save to CSV with the same naming pattern as Sentinel-1
                        start_date_clean = start_date.replace('-', '')
                        end_date_clean = end_date.replace('-', '')
                        output_csv = os.path.join(base_out_dir, f"stats_ndvi_S2_{start_date_clean}_{end_date_clean}.csv")
                        
                        # Append to existing CSV if it exists, otherwise create new
                        if os.path.exists(output_csv):
                            df.to_csv(output_csv, mode='a', header=False, index=False)
                        else:
                            df.to_csv(output_csv, index=False)
                        
                        print(f"Saved NDVI statistics to {output_csv}")
                    else:
                        print(f"No valid statistics extracted from {out_dir}")
            
            logging.info("NDVI statistics extraction completed successfully.")
            
    except Exception as err:
        logging.error(f"Error during NDVI export: {err}", exc_info=True)
        print("An error occurred. Check ndvi_export.log for details.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
