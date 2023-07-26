#!/bin/bash


for lazfile in ~/staff-umbrella/ahn3_sim_data/*.LAZ; do
    filename=$(basename -- "$lazfile")
    filename="${filename%.*}"
    pdal info --metadata $lazfile > /scratch/anauman/escience/projects/nD-PC/ahn3_sim_data_headers/${filename}.json
done