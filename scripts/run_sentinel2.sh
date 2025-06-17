#!/usr/bin/env bash
set -euo pipefail

# Resolve directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

PYTHON_SCRIPT="$ROOT_DIR/sentinel2/cli.py"
CONFIG_FILE="$ROOT_DIR/config/config.yml"

# Parse command line arguments
EXTRACT_EXISTING=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --extract-existing)
            EXTRACT_EXISTING=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --extract-existing    Extract NDVI statistics from existing GeoTIFF files only"
            echo "  --help, -h           Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Export NDVI and extract statistics"
            echo "  $0 --extract-existing # Extract statistics from existing files only"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

if [ "$EXTRACT_EXISTING" = true ]; then
    echo "Extracting NDVI statistics from existing files..."
    python3 "$PYTHON_SCRIPT" --config "$CONFIG_FILE" --extract-existing-only
else
    echo "Running NDVI export with statistics extraction..."
    python3 "$PYTHON_SCRIPT" --config "$CONFIG_FILE" --extract_stats
fi
