# cli.py
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

    # Extract values
    aoi_path = cfg["input"]["aoi_path"]
    start_date = cfg["input"]["start_date"]
    end_date = cfg["input"]["end_date"]
    cloud = cfg["input"]["cloud_threshold"]
    out_dir = cfg["output"]["base_dir"]

    try:
        ndvi_exporter.export_ndvi(
            aoi_path=aoi_path,
            start_date=start_date,
            end_date=end_date,
            cloud_threshold=cloud,
            output_dir=out_dir,
        )
        logging.info("NDVI export completed successfully.")
    except Exception as err:
        logging.error(f"Error during NDVI export: {err}", exc_info=True)
        print("An error occurred. Check ndvi_export.log for details.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
