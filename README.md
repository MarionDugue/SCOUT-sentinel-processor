# SCOUT-sentinel-processor

A comprehensive preprocessing pipeline for Sentinel-1 and Sentinel-2 satellite data, designed for agricultural remote sensing applications.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Workflow](#workflow)

## Overview

SCOUT-sentinel-processor automates the acquisition, processing, and statistiacl extraction of relevant metrics of SAR and optical satellite data for crop monitoring.

### Key Capabilities

- **Sentinel-1 Processing**: Automated SAR data acquisition, SNAP preprocessing, and statistical extraction
- **Sentinel-2 Processing**: NDVI computation and export using Google Earth Engine
- **Multi-field Support**: Process individual agricultural fields or regions of interest
- **Configurable Workflows**: Modular design allowing selective execution of processing steps

## Features

### Sentinel-1
- Automated scene discovery via Copernicus Data Space
- Configurable satellite selection (S1A, S1B, or both), acquisition modes (IW, EW, SM) and polarizations (VV, VH, HH, HV)
- SNAP-based preprocessing with custom graphs
- Field subsetting and statistical extraction

### Sentinel-2
- Google Earth Engine integration for scalable processing
- Cloud probability filtering
- Daily-averaged NDVI computation
- Multi-field processing support
- Automated statistics extraction

### General
- YAML-based configuration management
- Support for multiple vector formats (KML, GeoJSON, Shapefile)

## Architecture

```
SCOUT-sentinel-processor/
├── config/                 # Configuration files
│   └── config.yml         # Main configuration
├── sentinel1/             # Sentinel-1 processing modules
│   ├── s1_find_ids.py     # Scene discovery
│   ├── s1_download_from_csv.py  # Data download
│   ├── subset_raster.py   # Field subsetting
│   ├── extract_stats.py   # Statistical extraction
│   └── snap_graphs/       # SNAP processing graphs
├── sentinel2/             # Sentinel-2 processing modules
│   ├── cli.py            # Command-line interface
│   ├── ndvi_exporter.py  # NDVI computation
│   └── extract_ndvi_stats.py  # Statistics extraction
├── scripts/               # Execution scripts
│   ├── run_sentinel1.sh  # bash script for running workflow of Sentinel-1 (backscatter and polarimetry examples)
│   └── run_sentinel2.sh  # bash script for running workflow of Sentinel-2 (NDVI)
├── data/                  # Output data directory
├── kml/                   # Area of interest files
└── logs/                  # Processing logs
```

## Installation

### Prerequisites

- Python 3.7+
- Google Earth Engine account and authentication
- Copernicus Data Space account
- SNAP (Sentinel Application Platform)

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Authenticate Google Earth Engine
earthengine authenticate

# Configure Copernicus credentials in config/config.yml
```

## Configuration

The system uses `config/config.yml` to manage all processing parameters:

```yaml
workflow:
  find_ids: 1          # Enable scene discovery
  download: 1          # Enable data download
  pre_process: 1       # Enable preprocessing
  subset: 1            # Enable field subsetting
  extract_metric: 1    # Enable statistics extraction

input:
  aoi_path: "../kml/Eitelsried_Fields_kml"      # Individual field KMLs
  aoi_path_total: "../kml/Eitelsried_Fields_total.kml"  # Combined AOI
  start_date: "2025-04-01"
  end_date: "2025-05-31"

output:
  base_dir: "../data"

sentinel1:
  satellite: "BOTH"    # S1A, S1B, or BOTH
  mode: "IW"           # IW, EW, SM
  level: "SLC"         # SLC, GRD
  polarisation: "VV+VH"  # VV, VH, HH, HV, VV+VH, HH+HV
  snap:
    gpt_path: "/path/to/snap/bin/gpt"
    dem_path: "/path/to/dem.tif"

sentinel2:
  cloud_threshold: 15
```

## Usage
Sentinel-1 and Sentinel-2 preprocessing are done independently and do not rely on one another aside from sharing the same area of interest and timespan inpotuted in the config file. 

### Sentinel-1 Processing

#### Complete Pipeline
```bash
cd scripts
./run_sentinel1.sh
```

#### Individual Steps
```bash
# Find available scenes
python sentinel1/s1_find_ids.py --config config/config.yml

# Download scenes
python sentinel1/s1_download_from_csv.py --config config/config.yml

# Subset to fields
python sentinel1/subset_raster.py --config config/config.yml

# Extract statistics
python sentinel1/extract_stats.py --config config/config.yml
```

### Sentinel-2 Processing

```bash
cd sentinel2

# NDVI export with statistics
python cli.py --config ../config/config.yml --extract_stats

# Extract statistics from existing files
python cli.py --config ../config/config.yml --extract-existing-only
```

## Workflow

### Sentinel-1 Workflow
1. **Scene Discovery**: Query Copernicus Data Space for available scenes
2. **Data Download**: Download selected scenes to local storage
3. **Preprocessing**: Apply SNAP processing graphs
4. **Field Subsetting**: Extract data for individual fields
5. **Statistics Extraction**: Compute statistical metrics

### Sentinel-2 Workflow
1. **Area Processing**: Process each field/area of interest
2. **Cloud Filtering**: Filter scenes by cloud probability
3. **NDVI Computation**: Calculate daily-averaged NDVI
4. **Data Export**: Export GeoTIFF files
5. **Statistics Extraction**: Compute field-level statistics

### Data Flow

```
Input KML → Scene Discovery → Download → Preprocessing → Subsetting → Statistics
     ↓
Field Boundaries → Cloud Filtering → NDVI Computation → Export → Analysis
```



