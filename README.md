
# About
This is a fork of the original PotreeConverter 2.0. The original repository can be found [here](https://github.com/potree/PotreeConverter).

This git branch enables the user to perform the potree converter on very large input data for which the required storage is not available.
Therefore, it runs on partitioned data and the partial outputs are seemlessly merged into a single output.

## Usage

### Building PotreeConverter

The tool can be built using the following commands:

```bash
git clone
cd PotreeConverter
mkdir build
cd build
cmake ..
make
```

The tool is built in the `build` directory. The executable is named `PotreeConverter`.

### Running PotreeConverter
The tool is designed to be run using a configuration file. An example of a configuration file is provided below. The comments in the configuration file explain the different options.


```ini
[DEFAULT]
#The expected compression ratio of the input data. This is used to estimate the required disk space for the temporary and output files.
LazCompressionRatio = 7
#The copier type. Only "local" is supported for now.
CopierType = local
#Path to the log file. The log file is saved in the given path.
LogFile = /path/to/log/file

[INPUT_OUTPUT]
#The input directory containing the laz files.
InputDir = /path/to/input/directory
#The output directory for the converted data. The directory is created if it does not exist.
OutputDir = /path/to/output/directory
#The directory that contains the headers for the laz files. This is used to determine the bounding box of the input data. The name of the headers files should be same as the laz files and must have ".json" extension.
LazHeadersDir = /path/to/laz/headers/directory
#The file that contains the partition ins CSV format. The file should have the following columns: "filename" and "partition_id". The "filename" column should contain the name of the laz file and the "partition_id" column should contain the partition number of the laz file. The partition_id should be a number between 0 and the number of partitions - 1.
PartitionsCSV = /path/to/partitions/csv/file

[TMP_STORAGE]
#The maximum temporary space available in bytes. Use python expression format.
MaxTmpSpaceAvailable = 600*(1024)*3
#The directory to store the temporary files. The directory is created if it does not exist.
TmpDir = /path/to/tmp/directory
#If present, this should be same as the InputDir. If not present, the input files are copied to the TmpDir from the InputDir.
TmpInputDir = /path/to/tmp/input/directory


[SCHEDULER]
#The path to the converter program.
ProgramPath = /home/anauman/escience/projects/nD-PC/PotreeConverter/build/PotreeConverterMPI
#The options to pass to the converter program. See below for different options
ConverterOptions = --encoding BROTLI
#sbatch parameters. If not present, the converter program is run locally. 
SbatchParameters = --job-name=PotreeConverterMPIAHN3_C69_counting_mpi --output=/home/anauman/escience/projects/nD-PC/PotreeConverter/sbatchOutput/PotreeConverterMPI_counting_mpi_%%j.out --error=/home/anauman/escience/projects/nD-PC/PotreeConverter/sbatchOutput/PotreeConverterMPI_counting_mpi_%%j.err --partition=compute --account=research-abe-aet --nodes=10 --ntasks-per-node=1 --cpus-per-task=30 --time=10:00:00 --mem=100G
```
Different program options can be passed to the converter program using the `ConverterOptions` parameter. The following options are supported:

```
-h, --help: Display help information
--encoding: Encoding type "BROTLI", "UNCOMPRESSED"(default)
-m, --method: Point sampling method "poisson" (default), "poisson_average", "random"
--attributes: Attributes in the output file
--threads: Number of threads to use
```
