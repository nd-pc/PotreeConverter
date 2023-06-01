#!/bin/bash

for lazfile in ~/staff-umbrella/ahn3/*.LAZ; do
    filename=$(basename -- "$lazfile")
    filename="${filename%.*}"
    head -c 65536 $lazfile > /scratch/anauman/escience/projects/nD-PC/ahn3/${filename}_header.LAZ
done