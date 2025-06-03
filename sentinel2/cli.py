import os
import glob
import argparse
import logging
import sys
import yaml
import ndvi_exporter

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    return parser.parse_args()

def make_absolute(path, base_dir):
    return path if os.path.isabs(path) else os.path.abspath(os.path.join(base_dir, path))

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
                s2_cp_collection=s2_cp_collection
            )

        logging.info("NDVI export completed successfully for all AOIs.")
    except Exception as err:
        logging.error(f"Error during NDVI export: {err}", exc_info=True)
        print("An error occurred. Check ndvi_export.log for details.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
