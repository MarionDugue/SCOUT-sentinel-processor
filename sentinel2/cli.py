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

def main():
    logging.basicConfig(filename="ndvi_export.log", level=logging.INFO)

    args = parse_args()

    # Load YAML config
    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    aoi_path = cfg["input"]["aoi_path"]
    start_date = cfg["input"]["start_date"]
    end_date = cfg["input"]["end_date"]
    cloud = cfg["input"]["cloud_threshold"]
    base_out_dir = cfg["output"]["base_dir"]

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
            # Create an output directory for each KML
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
            )

        logging.info("NDVI export completed successfully for all AOIs.")
    except Exception as err:
        logging.error(f"Error during NDVI export: {err}", exc_info=True)
        print("An error occurred. Check ndvi_export.log for details.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
