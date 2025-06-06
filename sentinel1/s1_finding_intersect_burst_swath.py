#!/usr/bin/env python3

import argparse
import sys
import os
import yaml
import logging
from pathlib import Path
import geopandas as gpd
import stsa
from datetime import datetime


def setup_logging(log_file=None):
    """Set up logging configuration"""
    log_format = "%(asctime)s [%(levelname)s] [PREPROCESS] %(message)s"

    # Always log to stdout/stderr to be captured by the bash script
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # If log_file is provided, also log to file
    if log_file:
        # Get the script's directory
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Check for existing 'logs' directory first
        logs_dir = os.path.join(script_dir, "logs")
        log_dir = os.path.join(script_dir, "log")

        if os.path.exists(logs_dir):
            log_dir = logs_dir
        else:
            # Create log directory if neither exists
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

        # Setup main log file in log directory
        log_filename = os.path.basename(log_file) if log_file else "preprocess.log"
        main_log_path = os.path.join(log_dir, log_filename)

        # Add file handler for main log
        file_handler = logging.FileHandler(main_log_path)
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)

        # Setup error scenes log file in log directory
        error_log_file = os.path.join(log_dir, "error_scenes.log")
        error_handler = logging.FileHandler(error_log_file)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(error_handler)


def load_config(config_file):
    """Load and validate configuration from YAML file"""
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        # Validate required sections
        required_sections = ["input", "sentinel1", "pre-processing"]
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required section '{section}' in config file")

        return config
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        sys.exit(1)


def get_aoi_path(config):
    """Get the AOI path from config and validate it exists"""
    aoi_path = os.path.join(
        os.path.dirname(config_file), config["input"]["aoi_path_total"]
    )
    if not os.path.exists(aoi_path):
        raise ValueError(f"AOI path does not exist: {aoi_path}")
    return aoi_path


def analyze_intersections(zip_path, aoi_path, polarization, log_file=None):
    """Analyze burst/swath intersections for a given S1 scene"""
    scene_name = os.path.basename(zip_path)
    try:
        # Initialize the analyzer with polarization from config
        s1 = stsa.TopsSplitAnalyzer(
            target_subswaths=["iw1", "iw2", "iw3"],
            polarization=polarization,
            verbose=False,
        )

        # Load the S1 scene
        logging.info(f"Loading S1 scene with polarization {polarization}: {zip_path}")
        try:
            s1.load_zip(zip_path=zip_path)
        except Exception as e:
            error_msg = f"Scene {scene_name}: Error loading S1 scene - {str(e)}"
            logging.error(error_msg)
            return None

        # Load and validate AOI
        logging.info(f"Loading AOI: {aoi_path}")
        try:
            aoi = gpd.read_file(aoi_path)

            # Validate AOI
            if aoi.empty:
                error_msg = (
                    f"Scene {scene_name}: AOI file is empty - no geometries found"
                )
                logging.error(error_msg)
                return None

            # Ensure AOI is in the correct CRS (WGS84)
            if aoi.crs is None:
                logging.warning("AOI has no CRS defined, assuming WGS84")
                aoi.set_crs(epsg=4326, inplace=True)
            elif aoi.crs != "EPSG:4326":
                logging.info(f"Reprojecting AOI from {aoi.crs} to EPSG:4326")
                aoi = aoi.to_crs(epsg=4326)

            # Ensure we have valid geometries
            if not all(aoi.geometry.is_valid):
                error_msg = f"Scene {scene_name}: AOI contains invalid geometries"
                logging.error(error_msg)
                return None

        except Exception as e:
            error_msg = f"Scene {scene_name}: Error loading AOI - {str(e)}"
            logging.error(error_msg)
            logging.error(
                "Please ensure the AOI file exists and is a valid GeoJSON/KML file"
            )
            return None

        # Find intersections
        try:
            intersections = s1.intersecting_bursts(aoi)
            if intersections is None or len(intersections) == 0:
                error_msg = f"Scene {scene_name}: No intersecting bursts found between AOI and scene"
                logging.error(error_msg)
                return None

            # Log results
            logging.info(f"Found {len(intersections)} intersecting bursts:")
            for iw, burst in intersections:
                logging.info(f"  IW: {iw}, Burst: {burst}")

            return intersections

        except Exception as e:
            error_msg = f"Scene {scene_name}: Error finding intersections - {str(e)}"
            logging.error(error_msg)
            return None

    except Exception as e:
        error_msg = f"Scene {scene_name}: Error in analyze_intersections - {str(e)}"
        logging.error(error_msg)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Find S1 burst/swath intersections with AOI"
    )
    parser.add_argument("--config", required=True, help="Path to config file")
    parser.add_argument("--zip", required=True, help="Path to S1 scene zip file")
    parser.add_argument("--log", help="Path to log file")
    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log)

    # Load config
    global config_file
    config_file = args.config
    config = load_config(config_file)

    # Get AOI path
    try:
        aoi_path = get_aoi_path(config)
    except ValueError as e:
        logging.error(str(e))
        sys.exit(1)

    # Use VH as the default polarization for the TopsSplitAnalyzer, which one doesn't really matter as long as its part of the scene
    polarization = "VH"

    # Analyze intersections
    intersections = analyze_intersections(args.zip, aoi_path, polarization, args.log)

    if intersections:
        # Output results in a format suitable for further processing
        for iw, burst in intersections:
            print(f"{iw} {burst}")
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
