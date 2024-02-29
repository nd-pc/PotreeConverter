#!/bin/sh

#SBATCH -o out_PotreeConverter.txt
#SBATCH --error error_PotreeConverter.txt
#SBATCH --open-mode=append
#SBATCH --partition=compute
#SBATCH --account=research-abe-aet
#SBATCH --nodes=1
#SBATCH --cpus-per-task=12
#SBATCH --ntasks-per-node=1
#SBATCH --time 480:00                      
#SBATCH --mem 100G     

<path to PotreeConverter binary> --encoding BROTLI -i <path to the folder containing AHN LAZ>  -o < output folder name. No need to create it>