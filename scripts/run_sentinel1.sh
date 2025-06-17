#!/bin/bash

set -euo pipefail
trap 'echo "[ERROR] Script interrupted"; exit 130' INT

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Config file path (relative to script location)
CONFIG_FILE="${SCRIPT_DIR}/../config/config.yml"

# Function to read YAML values using Python
get_yaml_value() {
    local key="$1"
    python3 -c "
import yaml
import sys
try:
    with open('$CONFIG_FILE', 'r') as f:
        config = yaml.safe_load(f)
    keys = '$key'.split('.')
    value = config
    for k in keys:
        value = value[k]
    if value is None:
        print('')
    elif isinstance(value, bool):
        print(str(value).lower())  # Convert True/False to true/false
    else:
        print(str(value))
except Exception as e:
    print(f'Error reading config: {e}', file=sys.stderr)
    sys.exit(1)
"
}

# Function to safely transform values
safe_transform() {
    local value="$1"
    local transform="$2"
    if [ -n "$value" ]; then
        echo "$value" | $transform
    else
        echo ""
    fi
}

# Function to format date (remove hyphens)
format_date() {
    local date="$1"
    if [ -n "$date" ]; then
        echo "$date" | tr -d '-'
    else
        echo ""
    fi
}

# Read base paths from config file
BASE_DIR="$(get_yaml_value scripts.base_dir)"
if [ -z "$BASE_DIR" ]; then
    echo "Error: Could not read base_dir from config file" >&2
    exit 1
fi

# Get the absolute path of the base directory
BASE_DIR_ABS="$(cd "${SCRIPT_DIR}/${BASE_DIR}" && pwd)"

# Define logging functions
log_info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$PROCESS_LOG"
}

log_error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$ERROR_LOG" "$PROCESS_LOG" >&2
}

# Create log directory and initialize logs
LOG_DIR="${BASE_DIR_ABS}/$(get_yaml_value logging.log_dir)"
ERROR_LOG="${LOG_DIR}/$(get_yaml_value logging.error_log)"
PROCESS_LOG="${LOG_DIR}/$(get_yaml_value logging.process_log)"
mkdir -p "$LOG_DIR"
: > "$ERROR_LOG"
: > "$PROCESS_LOG"

# Get script paths
FIND_IDS_SCRIPT="${SCRIPT_DIR}/../sentinel1/s1_find_ids.py"
DOWNLOAD_SCRIPT="${BASE_DIR_ABS}/$(get_yaml_value scripts.download_script)"

# Now we can use log_info since everything is set up
log_info "Starting S1 workflow with config: $CONFIG_FILE"

# Read workflow toggles
RUN_FIND_IDS="$(get_yaml_value workflow.find_ids)"
RUN_DOWNLOAD="$(get_yaml_value workflow.download)"
SKIP_EXISTING="$(get_yaml_value workflow.skip_existing)"
RUN_PREPROCESS="$(get_yaml_value workflow.pre_process)"
RUN_SUBSET="$(get_yaml_value workflow.subset)"
RUN_EXTRACT_METRIC="$(get_yaml_value workflow.extract_metric)"

# Log workflow configuration
log_info "Workflow configuration:"
log_info "  find_ids: $RUN_FIND_IDS"
log_info "  download: $RUN_DOWNLOAD"
log_info "  skip_existing: $SKIP_EXISTING"
log_info "  pre_process: $RUN_PREPROCESS"
log_info "  subset: $RUN_SUBSET"
log_info "  extract_metric: $RUN_EXTRACT_METRIC"

# Validate workflow toggles
for var in RUN_FIND_IDS RUN_DOWNLOAD SKIP_EXISTING RUN_PREPROCESS RUN_SUBSET RUN_EXTRACT_METRIC; do
    if ! [[ "${!var}" =~ ^[01]$ ]]; then
        log_error "Invalid value for $var: ${!var}. Must be 0 or 1."
        exit 1
    fi
done

# Log file paths and configuration
log_info "File paths:"
log_info "  Config file: $CONFIG_FILE"
log_info "  Find IDs script: $FIND_IDS_SCRIPT"
log_info "  Download script: $DOWNLOAD_SCRIPT"
log_info "  Log directory: $LOG_DIR"
log_info "  Error log: $ERROR_LOG"
log_info "  Process log: $PROCESS_LOG"

# Construct CSV filename from config parameters
CSV_PATTERN="$(get_yaml_value output.s1_csv_pattern)"
SATELLITE="$(get_yaml_value sentinel1.satellite)"
MODE="$(get_yaml_value sentinel1.mode)"
LEVEL="$(get_yaml_value sentinel1.level)"
POLARISATION="$(safe_transform "$(get_yaml_value sentinel1.polarisation)" "tr -d '+'")"
START_DATE="$(format_date "$(get_yaml_value input.start_date)")"
END_DATE="$(format_date "$(get_yaml_value input.end_date)")"

# Get rel_orbit if it exists, otherwise use empty string
REL_ORBIT=""
if python3 -c "
import yaml
import sys
try:
    with open('$CONFIG_FILE', 'r') as f:
        config = yaml.safe_load(f)
    if 'sentinel1' in config and 'rel_orbit' in config['sentinel1']:
        print(str(config['sentinel1']['rel_orbit']))
except Exception:
    pass
"; then
    REL_ORBIT="$(python3 -c "
import yaml
import sys
try:
    with open('$CONFIG_FILE', 'r') as f:
        config = yaml.safe_load(f)
    if 'sentinel1' in config and 'rel_orbit' in config['sentinel1']:
        print(str(config['sentinel1']['rel_orbit']))
except Exception:
    pass
")"
fi

# Verify all required values are present
for var in CSV_PATTERN SATELLITE MODE LEVEL POLARISATION START_DATE END_DATE; do
    if [ -z "${!var}" ]; then
        echo "Error: Required value '$var' is empty in config file" >&2
        exit 1
    fi
done

# Log CSV configuration
log_info "CSV configuration:"
log_info "  Satellite: $SATELLITE"
log_info "  Mode: $MODE"
log_info "  Level: $LEVEL"
log_info "  Polarisation: $POLARISATION"
log_info "  Start date: $START_DATE"
log_info "  End date: $END_DATE"
log_info "  Relative orbit: ${REL_ORBIT:-ALL}"

# Construct CSV filename from config parameters
CSV_FILENAME="${CSV_PATTERN}"
CSV_FILENAME="${CSV_FILENAME/{satellite\}/$SATELLITE}"
CSV_FILENAME="${CSV_FILENAME/{mode\}/$MODE}"
CSV_FILENAME="${CSV_FILENAME/{level\}/$LEVEL}"
CSV_FILENAME="${CSV_FILENAME/{polarisation\}/$POLARISATION}"
CSV_FILENAME="${CSV_FILENAME/{start\}/$START_DATE}"
CSV_FILENAME="${CSV_FILENAME/{end\}/$END_DATE}"
CSV_FILENAME="${CSV_FILENAME/{rel_orbit\}/$([ -n "$REL_ORBIT" ] && echo "$REL_ORBIT" || echo "ALL")}"

# Construct full CSV file path
OUTPUT_BASE_DIR="$(get_yaml_value output.base_dir)"
if [ -z "$OUTPUT_BASE_DIR" ]; then
    echo "Error: Could not read output.base_dir from config file" >&2
    exit 1
fi

# Remove any "../" from the output directory path and get absolute path
OUTPUT_DIR_ABS="$(cd "${BASE_DIR_ABS}" && cd "$(dirname "${OUTPUT_BASE_DIR#../}")" && pwd)/$(basename "${OUTPUT_BASE_DIR#../}")"
CSV_FILE="${OUTPUT_DIR_ABS}/${CSV_FILENAME}"

# Read DEM path from config
DEM_PATH="$(get_yaml_value sentinel1.snap.dem_path)"
if [ -z "$DEM_PATH" ]; then
    log_error "DEM path not found in config file"
    exit 1
fi

# Verify DEM file exists
if [ ! -f "$DEM_PATH" ]; then
    log_error "DEM file not found at: $DEM_PATH"
    exit 1
fi

log_info "Using DEM file: $DEM_PATH"

# Function to run find_ids script
run_find_ids() {
    log_info "Starting find_ids step..."
    log_info "Running command: python3 $FIND_IDS_SCRIPT --config $CONFIG_FILE --log find_s1_ids.log"
    if python3 "$FIND_IDS_SCRIPT" --config "$CONFIG_FILE" --log "find_s1_ids.log" \
        2> >(tee -a "$ERROR_LOG" >&2); then
        log_info "Find IDs step completed successfully"
        if [ -f "$CSV_FILE" ]; then
            num_ids=$(wc -l < "$CSV_FILE")
            log_info "Found $((num_ids - 1)) scene IDs in $CSV_FILE"
        else
            log_error "CSV file was not created: $CSV_FILE"
        fi
        return 0
    else
        log_error "Find IDs step failed"
        return 1
    fi
}

# Function to check if scene exists
scene_exists() {
    local product_id="$1"
    local download_dir="$(get_yaml_value sentinel1.download_dir)"
    # Check for both .zip and .SAFE directories
    [ -f "${download_dir}/${product_id}.zip" ] || [ -d "${download_dir}/${product_id}.SAFE" ]
}

# Function to run download script
run_download() {
    local product_id="$1"
    local download_dir="$(get_yaml_value sentinel1.download_dir)"
    
    log_info "Processing product: $product_id"
    log_info "  Download directory: $download_dir"
    
    # Skip if scene exists and skip_existing is enabled
    if (( SKIP_EXISTING )) && scene_exists "$product_id"; then
        log_info "Skipping existing scene: $product_id"
        return 0
    fi

    log_info "Starting download for product: $product_id"
    log_info "Running command: python3 $DOWNLOAD_SCRIPT --product_id $product_id --config $CONFIG_FILE"
    if python3 "$DOWNLOAD_SCRIPT" --product_id "$product_id" --config "$CONFIG_FILE" \
        2> >(tee -a "$ERROR_LOG" >&2); then
        log_info "Download successful for product: $product_id"
        if scene_exists "$product_id"; then
            log_info "Verified product exists after download: $product_id"
        else
            log_error "Product not found after download: $product_id"
        fi
        return 0
    else
        log_error "Download failed for product: $product_id"
        return 1
    fi
}

# Function to run download for all products
run_download_all() {
    if [ ! -f "$CSV_FILE" ]; then
        log_error "CSV file not found: $CSV_FILE"
        return 1
    fi
    
    log_info "Starting download for all products from: $CSV_FILE"
    
    # Skip header line and process each product
    local success_count=0
    local total_count=0
    
    while IFS=',' read -r product_id rest; do
        # Skip header line
        if [[ "$product_id" == "product_id" ]]; then
            continue
        fi
        
        # Skip empty lines
        if [ -z "$product_id" ]; then
            continue
        fi
        
        ((total_count++))
        if run_download "$product_id"; then
            ((success_count++))
        fi
    done < "$CSV_FILE"
    
    log_info "Download summary: $success_count of $total_count products processed successfully"
    
    if [ $success_count -eq $total_count ]; then
        return 0
    else
        return 1
    fi
}

# Function to run pre-processing (burst/swath intersection analysis and SNAP processing)
run_preprocess() {
    local product_id="$1"
    local scene_name="$2"
    local download_dir="$(get_yaml_value sentinel1.download_dir)"
    local gpt_path="$(get_yaml_value sentinel1.snap.gpt_path)"
    local zip_file="${download_dir}/${scene_name}.SAFE.zip"
    
    # Get output directories from config
    local intermediate_dir="${download_dir}/intermediate"
    local final_dir="$(get_yaml_value sentinel1.snap.output_dir)"
    # Replace {download_dir} with actual download directory
    final_dir="${final_dir/\{download_dir\}/$download_dir}"
    mkdir -p "$intermediate_dir" "$final_dir"
    
    log_info "Starting pre-processing for product: $product_id"
    log_info "Debug information:"
    log_info "  Download directory: $download_dir"
    log_info "  Scene name: $scene_name"
    log_info "  Full zip path: $zip_file"
    log_info "  Intermediate directory: $intermediate_dir"
    log_info "  Final output directory: $final_dir"
    
    # Check if download directory exists
    if [ ! -d "$download_dir" ]; then
        log_error "Download directory does not exist: $download_dir"
        return 1
    fi
    
    # Check for exact file
    if [ ! -f "$zip_file" ]; then
        log_error "ZIP file not found: $zip_file"
        # Try to find similar files
        log_info "Searching for similar files:"
        find "$download_dir" -maxdepth 1 -type f -name "*${scene_name}*" | while read -r found_file; do
            log_info "  Found similar file: $found_file"
        done
        return 1
    fi

    # Get swath and burst information
    log_info "Finding intersecting burst and swath..."
    SWATH_BURST=$(python3 "${SCRIPT_DIR}/../sentinel1/s1_finding_intersect_burst_swath.py" \
        --config "$CONFIG_FILE" \
        --zip "$zip_file" \
        --log "preprocess.log" 2>/dev/null | grep -E '^IW[1-3] [0-9]+$') || {
        log_error "Failed to get swath and burst information"
        return 1
    }

    # Extract just the swath name and burst number
    SWATH=$(echo "$SWATH_BURST" | awk '{print $1}')
    BURST=$(echo "$SWATH_BURST" | awk '{print $2}')
    
    if [[ -z "$SWATH" || -z "$BURST" ]]; then
        log_error "Could not extract swath or burst for scene: $scene_name"
        return 1
    fi

    # Validate swath and burst format
    if ! [[ "$SWATH" =~ ^IW[1-3]$ ]]; then
        log_error "Invalid swath format: $SWATH. Expected IW1, IW2, or IW3"
        return 1
    fi
    
    if ! [[ "$BURST" =~ ^[0-9]+$ ]]; then
        log_error "Invalid burst format: $BURST. Expected a number"
        return 1
    fi

    log_info "Found swath: $SWATH, burst: $BURST"

    # Get the graphs configuration
    local graphs_json
    graphs_json=$(python3 -c "
import yaml, json, sys, os
with open('$CONFIG_FILE', 'r') as f:
    config = yaml.safe_load(f)
graphs = config['sentinel1']['snap']['graphs']
# Convert relative paths to absolute paths using script directory
for graph in graphs:
    if not os.path.isabs(graph['path']):
        # Remove any leading 'sentinel1/' from the path since we're now in scripts directory
        path = graph['path']
        if path.startswith('sentinel1/'):
            path = path[len('sentinel1/'):]
        graph['path'] = os.path.abspath(os.path.join('$SCRIPT_DIR', '..', 'sentinel1', path))
print(json.dumps(graphs))
")
    
    # Process each SNAP graph in sequence
    local current_input="$zip_file"
    local current_output=""
    
    echo "$graphs_json" | python3 -c "
import json, sys, os
graphs = json.load(sys.stdin)
for i, graph in enumerate(graphs):
    print(f'{graph[\"path\"]}|{graph[\"output_suffix\"]}')
" | while IFS='|' read -r graph_path output_suffix; do
        # Set output path - save both intermediate and final outputs on the server
        if [[ "$output_suffix" == *"_final"* ]]; then #the output_suffix has to contain '_final' to be put in the correct folder
            current_output="${final_dir}/${scene_name}${output_suffix}"
        else
            current_output="${intermediate_dir}/${scene_name}${output_suffix}"
        fi
        
        # Skip if output exists and skip_existing is enabled
        if (( SKIP_EXISTING )) && [ -f "$current_output" ]; then
            log_info "Skipping existing SNAP output: $current_output"
            current_input="$current_output"
            continue
        fi
        
        # Run SNAP graph
        log_info "Running SNAP graph: $(basename "$graph_path")"
        log_info "  Full graph path: $graph_path"
        log_info "  Input: $current_input"
        log_info "  Output: $current_output"
        
        # Verify graph file exists before running
        if [ ! -f "$graph_path" ]; then
            log_error "SNAP graph file not found: $graph_path"
            return 1
        fi

        if "$gpt_path" "$graph_path" \
            -Pinput_file="$current_input" \
            -PselectedSwath="$SWATH" \
            -PburstIndex="$BURST" \
            -Poutput_file="$current_output" \
            -Pdem_path="$DEM_PATH" \
            -c 8G -q 8 -x; then
            log_info "SNAP graph completed successfully"
            current_input="$current_output"
        else
            log_error "SNAP graph failed"
            return 1
        fi
    done
    
    log_info "Pre-processing completed successfully for product: $product_id"
    return 0
}

# Function to run pre-processing for all products
run_preprocess_all() {
    if [ ! -f "$CSV_FILE" ]; then
        log_error "CSV file not found: $CSV_FILE"
        return 1
    fi
    
    log_info "Starting pre-processing for all products from: $CSV_FILE"
    
    # Skip header line and process each product
    local success_count=0
    local total_count=0
    
    while IFS=',' read -r product_id rest; do
        # Skip header line
        if [[ "$product_id" == "product_id" ]]; then
            continue
        fi
        
        # Skip empty lines
        if [ -z "$product_id" ]; then
            continue
        fi
        
        ((total_count++))
        if run_preprocess "$product_id" "$product_id"; then
            ((success_count++))
        fi
    done < "$CSV_FILE"
    
    log_info "Pre-processing summary: $success_count of $total_count products processed successfully"
    
    if [ $success_count -eq $total_count ]; then
        return 0
    else
        return 1
    fi
}

# Function to run subsetting
run_subset() {
    if (( ! RUN_SUBSET )); then
        log_info "Skipping subsetting step (disabled in config)"
        return 0
    fi

    log_info "Starting subsetting step..."
    local subset_script="${SCRIPT_DIR}/../sentinel1/$(get_yaml_value subsetting.scripts.subset_script)"
    local subset_log="${SCRIPT_DIR}/../sentinel1/$(get_yaml_value subsetting.logs.subset_log)"
    
    if python3 "$subset_script" --config "$CONFIG_FILE" --log "$subset_log" 2> >(tee -a "$ERROR_LOG" >&2); then
        log_info "Subsetting step completed successfully"
        return 0
    else
        log_error "Subsetting step failed"
        return 1
    fi
}

# Function to run metric extraction
run_extract_metric() {
    if (( ! RUN_EXTRACT_METRIC )); then
        log_info "Skipping metric extraction step (disabled in config)"
        return 0
    fi

    log_info "Starting metric extraction step..."
    local stats_script="${SCRIPT_DIR}/../sentinel1/$(get_yaml_value subsetting.scripts.stats_script)"
    local stats_log="${SCRIPT_DIR}/../sentinel1/$(get_yaml_value subsetting.logs.stats_log)"
    local download_dir="$(get_yaml_value sentinel1.download_dir)"
    
    # Get subset directory from config
    local subset_dir="$(get_yaml_value subsetting.output_dir)"
    # Replace {download_dir} with actual download directory
    subset_dir="${subset_dir/\{download_dir\}/$download_dir}"
    
    # Get output directory from config
    local output_dir="${BASE_DIR_ABS}/$(get_yaml_value output.base_dir)"
    mkdir -p "$output_dir"
    
    # Get config values for filename construction
    local satellite="$(get_yaml_value sentinel1.satellite)"
    local mode="$(get_yaml_value sentinel1.mode)"
    local level="$(get_yaml_value sentinel1.level)"
    local polarisation="$(get_yaml_value sentinel1.polarisation | tr -d '+')"
    local start_date="$(get_yaml_value input.start_date | tr -d '-')"
    local end_date="$(get_yaml_value input.end_date | tr -d '-')"
    local rel_orbit="$(get_yaml_value sentinel1.rel_orbit)"
    rel_orbit="${rel_orbit:-ALL}"  # Default to ALL if not set
    
    # Construct base filename pattern
    local filename_pattern="stats_{type}_S1_${satellite}_${mode}_${level}_${polarisation}_${start_date}_${end_date}_orbit${rel_orbit}.csv"
    
    # Create full paths for both CSV files
    local stats_csv_dB="${output_dir}/${filename_pattern/{type\}/dB}"
    local stats_csv_poldecomp="${output_dir}/${filename_pattern/{type\}/poldecomp}"
    
    log_info "Output files:"
    log_info "  dB stats: $stats_csv_dB"
    log_info "  poldecomp stats: $stats_csv_poldecomp"
    
    # Create log directory
    mkdir -p "$(dirname "$stats_log")"
    
    # Check if subset directory exists
    if [ ! -d "$subset_dir" ]; then
        log_error "Subset directory not found: $subset_dir"
        return 1
    fi
    
    # Process each field directory
    local success_count=0
    local total_count=0
    
    # Skip hidden directories (those starting with ._)
    for field_dir in "$subset_dir"/[^._]*/; do
        if [ ! -d "$field_dir" ]; then
            continue
        fi
        
        local field_id=$(basename "$field_dir")
        log_info "Processing field: $field_id"
        
        # Process dB files
        while IFS= read -r input_file; do
            ((total_count++))
            local filename=$(basename "$input_file")
            
            # Extract scene_id and acquisition time from filename
            if [[ $filename =~ (S1[AB]_IW_SLC__[0-9A-Z]+_[0-9]{8}T[0-9]{6}_[0-9]{8}T[0-9]{6}_[0-9]+_[0-9A-F]+_[0-9A-F]+) ]]; then
                local scene_id="${BASH_REMATCH[1]}"
                local raw_time="${BASH_REMATCH[1]##*_}"
                # Extract the first timestamp from the scene_id for acquisition time
                if [[ $scene_id =~ _([0-9]{8}T[0-9]{6})_ ]]; then
                    local first_timestamp="${BASH_REMATCH[1]}"
                    local acquisition_time="${first_timestamp:0:4}-${first_timestamp:4:2}-${first_timestamp:6:2}T${first_timestamp:9:2}:${first_timestamp:11:2}:${first_timestamp:13:2}"
                else
                    log_error "Could not extract acquisition time from scene_id: $scene_id"
                    continue
                fi
                
                log_info "Processing dB file: $filename"
                log_info "  Scene ID: $scene_id"
                log_info "  Acquisition time: $acquisition_time"
                
                if python3 "$stats_script" \
                    --config "$CONFIG_FILE" \
                    --input "$input_file" \
                    --scene_id "$scene_id" \
                    --field_id "$field_id" \
                    --acquisition_time "$acquisition_time" \
                    --output_csv "$stats_csv_dB" \
                    --log "$stats_log" 2> >(tee -a "$ERROR_LOG" >&2); then
                    ((success_count++))
                    log_info "Successfully processed dB file: $filename"
                else
                    log_error "Failed to process dB file: $filename"
                fi
            else
                log_error "Could not extract scene ID from filename: $filename"
            fi
        done < <(find "$field_dir" -type f -name "*_dB_final*.tif")
        
        # Process poldecomp files
        while IFS= read -r input_file; do
            ((total_count++))
            local filename=$(basename "$input_file")
            
            # Extract scene_id and acquisition time from filename
            if [[ $filename =~ (S1[AB]_IW_SLC__[0-9A-Z]+_[0-9]{8}T[0-9]{6}_[0-9]{8}T[0-9]{6}_[0-9]+_[0-9A-F]+_[0-9A-F]+) ]]; then
                local scene_id="${BASH_REMATCH[1]}"
                local raw_time="${BASH_REMATCH[1]##*_}"
                # Extract the first timestamp from the scene_id for acquisition time
                if [[ $scene_id =~ _([0-9]{8}T[0-9]{6})_ ]]; then
                    local first_timestamp="${BASH_REMATCH[1]}"
                    local acquisition_time="${first_timestamp:0:4}-${first_timestamp:4:2}-${first_timestamp:6:2}T${first_timestamp:9:2}:${first_timestamp:11:2}:${first_timestamp:13:2}"
                else
                    log_error "Could not extract acquisition time from scene_id: $scene_id"
                    continue
                fi
                
                log_info "Processing poldecomp file: $filename"
                log_info "  Scene ID: $scene_id"
                log_info "  Acquisition time: $acquisition_time"
                
                if python3 "$stats_script" \
                    --config "$CONFIG_FILE" \
                    --input "$input_file" \
                    --scene_id "$scene_id" \
                    --field_id "$field_id" \
                    --acquisition_time "$acquisition_time" \
                    --output_csv "$stats_csv_poldecomp" \
                    --log "$stats_log" 2> >(tee -a "$ERROR_LOG" >&2); then
                    ((success_count++))
                    log_info "Successfully processed poldecomp file: $filename"
                else
                    log_error "Failed to process poldecomp file: $filename"
                fi
            else
                log_error "Could not extract scene ID from filename: $filename"
            fi
        done < <(find "$field_dir" -type f -name "*_poldecomp_final*.tif")
    done
    
    # Log summary
    if [ $total_count -eq 0 ]; then
        log_error "No subset files found in: $subset_dir"
        return 1
    elif [ $success_count -eq $total_count ]; then
        log_info "Successfully processed all $total_count files"
        return 0
    else
        log_error "Partially successful: processed $success_count of $total_count files"
        return 1
    fi
}

# Main workflow execution
if (( RUN_FIND_IDS )); then
    run_find_ids || exit 1
fi

if (( RUN_DOWNLOAD )); then
    run_download_all || exit 1
fi

if (( RUN_PREPROCESS )); then
    run_preprocess_all || exit 1
fi

if (( RUN_SUBSET )); then
    run_subset || exit 1
fi

if (( RUN_EXTRACT_METRIC )); then
    run_extract_metric || exit 1
fi

log_info "Workflow completed successfully"
exit 0 