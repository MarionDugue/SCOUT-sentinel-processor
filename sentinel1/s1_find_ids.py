import requests
import pandas as pd
import geopandas as gpd
import yaml
from shapely.geometry import MultiPolygon
from datetime import datetime
import os
import glob
import argparse
import logging

# Constants moved from config file
POLARISATION_MAPPING = {
    "1SDV": "VV+VH",
    "1SDH": "HH+HV", 
    "1SSV": "VV",
    "1SSH": "HH"
}

REMOVE_SUFFIX = ".SAFE"
EMPTY_WKT = "MULTIPOLYGON EMPTY"

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [FIND_IDS] %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--log", default="find_s1_ids.log", help="Log file name")
    return parser.parse_args()


def make_absolute(path, base_dir):
    return (
        path if os.path.isabs(path) else os.path.abspath(os.path.join(base_dir, path))
    )


def extract_wkt_from_kml(kml_path: str, layer: str = None) -> str:
    if os.path.isdir(kml_path):
        kml_files = glob.glob(os.path.join(kml_path, "*.kml"))
        if not kml_files:
            raise FileNotFoundError(f"No .kml file found in directory: {kml_path}")
        kml_path = kml_files[0]  # pick first match, or handle multiple if needed

    gdf = gpd.read_file(kml_path, driver="KML", layer=layer)

    if gdf.empty or gdf[gdf.geometry.type.isin(["Polygon", "MultiPolygon"])].empty:
        return EMPTY_WKT

    multipoly = gdf[
        gdf.geometry.type.isin(["Polygon", "MultiPolygon"])
    ].geometry.union_all()
    if isinstance(multipoly, MultiPolygon):
        return multipoly.wkt
    return MultiPolygon([multipoly]).wkt


def get_s1_data(wkt: str, config: dict) -> pd.DataFrame:
    s1 = config["sentinel1"]
    input_dates = config["input"]
    odata_cfg = s1.get("odata", {})
    base_url = odata_cfg.get("base_url")

    contains = (
        f"(contains(Name, 'S1A_{s1['mode']}_{s1['level']}') or contains(Name, 'S1B_{s1['mode']}_{s1['level']}'))"
        if s1["satellite"] == "BOTH"
        else f"contains(Name, '{s1['satellite']}_{s1['mode']}_{s1['level']}')"
    )

    filter_params = [
        f"Collection/Name eq '{s1['collection']}'",
        contains,
        f"OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')",
        f"ContentDate/Start gt {input_dates['start_date']}T00:00:00.000Z",
        f"ContentDate/Start lt {input_dates['end_date']}T00:00:00.000Z",
    ]

    params = {
        "$filter": " and ".join(filter_params),
        "$orderby": s1.get("odata", {}).get("orderby", "ContentDate/Start"),
        "$top": str(s1.get("odata", {}).get("top", 1000)),
    }

    response = requests.get(url=base_url, params=params)
    response.raise_for_status()
    df = pd.DataFrame(response.json().get("value", []))

    if "ContentDate" in df.columns:
        df["timestamp"] = pd.to_datetime(df["ContentDate"].apply(lambda x: x["Start"]))
    else:
        df["timestamp"] = pd.NaT

    data = df["Name"].astype(str).str.replace("__", "_")
    split_data = data.str.split("_", expand=True)
    naming_columns = [
        "satellite",
        "mode",
        "TTTR",
        "polarisation",
        "Start_DT",
        "Stop_DT",
        "Absolute_Orbit",
        "Mission_ID",
        "Product_ID",
    ]
    for i, col in enumerate(naming_columns[: split_data.shape[1]]):
        df[col] = split_data[i]

    df["orbit"] = df["timestamp"].apply(
        lambda x: "Descending" if x.hour in [6] else "Ascending"
    )
    df["Absolute_Orbit"] = df["Absolute_Orbit"].astype(int)

    df.loc[df["satellite"] == "S1A", "relat_orbit"] = (
        (df["Absolute_Orbit"] - 73) % 175
    ) + 1
    df.loc[df["satellite"] == "S1B", "relat_orbit"] = (
        (df["Absolute_Orbit"] - 27) % 175
    ) + 1

    if "rel_orbit" in s1:
        df = df[df["relat_orbit"] == s1["rel_orbit"]]

    if "polarisation" in s1:
        df["polarisation"] = df["polarisation"].replace(POLARISATION_MAPPING)
        df = df[df["polarisation"] == s1["polarisation"]]

    df = df[~df["Name"].str.contains("COG")]
    return df


def main():
    args = parse_args()
    config_path = os.path.abspath(args.config)
    config_dir = os.path.dirname(config_path)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    input_cfg = config["input"]
    output_cfg = config["output"]
    s1_cfg = config.get("sentinel1", {})

    aoi_path = make_absolute(input_cfg["aoi_path_total"], config_dir)
    base_out_dir = make_absolute(output_cfg["base_dir"], config_dir)

    # Parse dates
    start_dt = datetime.strptime(input_cfg["start_date"], "%Y-%m-%d")
    end_dt = datetime.strptime(input_cfg["end_date"], "%Y-%m-%d")
    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")

    # Extract params from config with defaults
    satellite = s1_cfg.get("satellite", "BOTH")
    mode = s1_cfg.get("mode", "IW")
    level = s1_cfg.get("level", "SLC")
    polarisation = s1_cfg.get("polarisation", "").replace("+", "")
    rel_orbit = s1_cfg.get("rel_orbit", None)  # can be None if not set

    # Prepare pattern and formatting dictionary
    pattern = output_cfg.get("s1_csv_pattern", None)

    format_dict = {
        "satellite": satellite,
        "mode": mode,
        "level": level,
        "polarisation": polarisation,
        "start": start_str,
        "end": end_str,
        # If rel_orbit is None, replace with "all" or empty string
        "rel_orbit": str(rel_orbit) if rel_orbit is not None else "ALL",
    }

    if pattern:
        csv_name = pattern.format(**format_dict)
    else:
        csv_name = output_cfg.get("s1_csv", "s1_ids.csv")

    output_csv_path = os.path.join(base_out_dir, csv_name)

    wkt = extract_wkt_from_kml(aoi_path)

    df = get_s1_data(wkt=wkt, config=config)

    if df.empty:
        logging.warning(" No matching Sentinel-1 scenes found.")
        return

    # Remove '.SAFE' only from names, if you still need it elsewhere
    df["Name"] = df["Name"].str.replace(REMOVE_SUFFIX, "", regex=False)

    # Save the 'Id' column (the UUIDs) instead of 'Name'
    df[["Id", "Name"]].to_csv(output_csv_path, index=False)

    logging.info(" Saved %d scene UUIDs to %s", len(df), output_csv_path)


if __name__ == "__main__":
    args = parse_args()
    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    log_path = os.path.join(log_dir, args.log)
    setup_logger(log_path)

    logging.info("Logger initialized")
    main()
