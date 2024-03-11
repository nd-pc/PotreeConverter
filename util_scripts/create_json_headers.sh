#!/bin/bash

path_to_laz_dir=$1
path_to_laz_headers_dir=$2

# Check if PDAL is installed
if ! command -v pdal &> /dev/null; then
    echo "Error: PDAL is not installed. Please install PDAL before running this script."
    exit 1
fi

if [ -z "$path_to_laz_dir" ]; then
    echo "Please provide the path to the input LAZ files directory."
    exit 1
fi



mkdir -p "$path_to_laz_headers_dir"

for lazfile in "$path_to_laz_dir"/*.{laz,LAZ,Laz}; do
    filename=$(basename -- "$lazfile")
    filestemname="${filename%.*}"
    pdal info --metadata "$lazfile" > "$path_to_laz_headers_dir/${filestemname}.json"
    echo "Header for $lazfile written to $path_to_laz_headers_dir/${filestemname}.json"
done
