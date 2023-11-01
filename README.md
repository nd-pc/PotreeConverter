
# About
This is a fork of the original PotreeConverter 2.0. The original repository can be found [here](https://github.com/potree/PotreeConverter).

This git branch enables the user to perform the potree converter on very large input data for which the required storage is not available.
Therefore, it runs on partitioned data and the partial outputs are seemlessly merged into a single output.

## Usage

### Building the tool

The tool can be built using the following commands:

```bash
git clone
cd PotreeConverter
mkdir build
cd build
cmake ..
make
```

The tool is designed to be run using a configuration file. An example of a configuration file is provided below. The comments in the configuration file explain the different options.


```ini
[DEFAULT]
#The expected compression ratio of the input data. This is used to estimate the required disk space for the temporary and output files.
LazCompressionRatio = 7
#The copier type. Only "local" is supported for now.
CopierType = local
#Path to the log file to save
LogFile = /home/anauman/escience/projects/nD-PC/PotreeConverter/logs/PotreeConverterBatchedAHN3_C69_counting_mpi.log

[INPUT_OUTPUT]
#The input directory containing the laz files.
InputDir = /home/anauman/staff-umbrella/ahn3
#The output directory for the converted data.
OutputDir = /home/anauman/staff-umbrella/ahn3_C69_PotreeConverterMPI_output_counting_mpi
#The directory that contains the headers for the laz files. This is used to determine the bounding box of the input data. The name of the headers files should be same as the laz files and must have ".json" extension.
LazHeadersDir = /scratch/anauman/escience/projects/nD-PC/ahn3_C69_headers
#The file that contains the partition ins CSV format. The file should have the following columns: "filename" and "partition_id". The "filename" column should contain the name of the laz file and the "partition_id" column should contain the partition number of the laz file. The partition_id should be a number between 0 and the number of partitions - 1.
PartitionsCSV = /home/anauman/escience/projects/nD-PC/PotreeConverter/ahn3_C69_partitions_1x1.csv

[TMP_STORAGE]
#The maximum temporary space available in bytes
MaxTmpSpaceAvailable = 600*(1024)*3
#The directory to store the temporary files.
TmpDir = /scratch/anauman/escience/projects/nD-PC/PotreeConverterMPI_ahn3_C69_counting_mpi_tmp


# If you want to use the slurm scheduler to run the tool, uncomment the following section and provide the required parameters.
[SLURM_SCHEDULER]
#The path to the converter program.
ProgramPath = /home/anauman/escience/projects/nD-PC/PotreeConverter/build/PotreeConverterMPI
#The options to pass to the converter program. See below for different options
ConverterOptions = --encoding BROTLI
#sbatch parameters
SbatchParameters = --job-name=PotreeConverterMPIAHN3_C69_counting_mpi --output=/home/anauman/escience/projects/nD-PC/PotreeConverter/sbatchOutput/PotreeConverterMPI_counting_mpi_%%j.out --error=/home/anauman/escience/projects/nD-PC/PotreeConverter/sbatchOutput/PotreeConverterMPI_counting_mpi_%%j.err --partition=compute --account=research-abe-aet --nodes=10 --ntasks-per-node=1 --cpus-per-task=30 --time=10:00:00 --mem=100G
```
Different program options can be passed to the converter program using the `ConverterOptions` parameter. The following options are supported:

```bash
--e
