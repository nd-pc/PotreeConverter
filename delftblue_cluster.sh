#!/bin/sh

#SBATCH -o out_PotreeConverterMPI.txt
#SBATCH --error error_PotreeConverterMPI.txt
#SBATCH --open-mode=append
#SBATCH --partition=compute
#SBATCH --account=research-abe-aet
#SBATCH --nodes=2
#SBATCH --cpus-per-task=12 # On Delftblue 1 cpu = 1 core. Hyperthreading is disabled 
#SBATCH --ntasks-per-node=2
#SBATCH --time 480:00                      	# Run time (mm:ss)
#SBATCH --mem 100G			# Memory


#/home/anauman/escience/projects/nD-PC/PotreeConverter/build/PotreeConverter --encoding BROTLI -i /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AN1.LAZ /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AN2.LAZ /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AZ1.LAZ /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AZ2.LAZ  -o /scratch/anauman/escience/projects/nD-PC/PotreeConverter_output 


mpiexec /home/anauman/escience/projects/nD-PC/PotreeConverter/build/PotreeConverterMPI --encoding BROTLI -i echo $(echo $(ls /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AN1.LAZ))  -o /scratch/anauman/escience/projects/nD-PC/PotreeConverter_output
#timepid=$!
#potreepid=$(pgrep -P $timepid)

#/home/anauman/escience/projects/nD-PC/PotreeConverter/build/PotreeConverter -i /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AN1.LAZ /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AN2.LAZ /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AZ1.LAZ /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AZ2.LAZ -o /scratch/anauman/escience/projects/nD-PC/PotreeConverter_output

#/home/anauman/escience/projects/nD-PC/PotreeConverter/build/PotreeConverter -i /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AN1.LAZ -o /scratch/anauman/escience/projects/nD-PC/PotreeConverter_output &


#python /home/anauman/escience/projects/nD-PC/PotreeConverter/ps_monitor.py $! /scratch/anauman/escience/projects/nD-PC/PotreeConverter_output





#--encoding BROTLI -i /scratch/anauman/escience/projects/nD-PC/ahn3/C_69AN1.LAZ  -o /scratch/anauman/escience/projects/nD-PC/PotreeConverter_output






