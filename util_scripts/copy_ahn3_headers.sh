#!/bin/bash

path_to_ahn3_dir=~/staff-umbrella/ahn3
path_to_ahn3_headers_dir=/scratch/anauman/escience/projects/nD-PC/ahn3_headers

for lazfile in $path_to_ahn3_dir/*.LAZ; do
    filename=$(basename -- "$lazfile")
    filestemname="${filename%.*}"
    pdal info --metadata $lazfile > $path_to_ahn3_headers_dir/${filestemname}.json
    echo "Header for $lazfile written to $path_to_ahn3_headers_dir/${filestemname}.json"
done