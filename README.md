
# About
This is a fork of the original PotreeConverter 2.0. The original repository can be found [here](https://github.com/potree/PotreeConverter).

This git branch enables the user to perform the potree converter on very large input data for which the required storage is not available.
Therefore, it runs on partitioned data and the partial outputs are seemlessly merged into a single output.

## Usage

### Requirements

The following packages are required to run the tool:

g++ >= 9.4.0

cmake >= 3.16.3

python >= 3.9.12

To extract headers from input LAZ files , install pdal >= 2.4.3 and use the following bash script:

```bash
#!/bin/bash

# Use the path to the actual path header directory
LAZ_DIR=/path/to/the/headers/directory

# Use the path to the actual input directory
INPUT_DIR=/path/to/the/input/directory

mkdir $LAZ_DIR



# Use the path to the actual input directory
for lazfile in $INPUT_DIR/*.LAZ; do
    filename=$(basename -- "$lazfile")
    filename="${filename%.*}"
    pdal info --metadata $lazfile > $LAZ_DIR/${filename}.json
done

```

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
#The amount of LAZ data to be batch processed in the counting phase. Use python expression format.
CountingBatchSize = 60*(1024)**3

[INPUT_OUTPUT]
#The input directory containing the laz files.
InputDir = /path/to/input/directory
#The input directory type. Must be one of "local", "remote". If "remote", the program assumes that the directory is not directlt accessible by the compute nodes and therefore input files are copied to the temporary directory. If "local", the input files are not copied to the temporary directory. The input files are directly accessed from the input directory.
InputDirType = local | remote
#The output directory for the converted data. The output directory is created if it does not exist.
OutputDir = /path/to/output/directory
#The output directory type. Must be one of "local", "remote". If "remote", the program assumes that the directory is not directlt accessible by the compute nodes and therefore output files are copied to the temporary directory which are then cpoied to the remote storage by the script. If "local", the output files are directly copied to the  output directory.
OutputDirType = local | remote
#The directory that contains the headers for the laz files. This is used to determine the bounding box of the input data. The name of the headers files should be same as the laz files and must have ".json" extension.
LazHeadersDir = /path/to/laz/headers/directory

[PARTITIONS]
#The file that contains the partition ins CSV format. The file should have the following columns: "filename" and "partition_id". The "filename" column should contain the name of the laz file and the "partition_id" column should contain the partition number of the laz file. The partition_id should be a number between 0 and the number of partitions - 1.
CSV = /path/to/partitions/csv/file
#The amount of LAZ data to be batch processed in the counting phase. Use python expression format.
CountingBatchSize = 60*(1024)**3

[TMP_STORAGE]
#The maximum temporary space available in bytes. Use python expression format.
MaxTmpSpaceAvailable = 600*(1024)*3
#The directory to store the temporary files. The directory is created if it does not exist.
TmpDir = /path/to/tmp/directory


[PROGRAM]
#The path to the converter program.
ProgramPath = /path/to/PotreeConverter
#The options to pass to the converter program. See below for different options. Empty string is passed in case of the default setting. The PotreeConverter Implementation in this branch is designed for very large input data. Therefore, it is always run with "--encoding BROTLI" option to compress the output.
Options = ""

[SCHEDULER]
#The scheduler type. Must be one of "sbatch", "pbs", "local".
Type = sbatch | pbs | local
Parameters = <scheduler parameters>

[COPIER]
#The copier type. Only "local" is supported for now.
CopierType = local
```
Different program options can be passed to the converter program using the `ConverterOptions` parameter. The following options are supported:

```
-m, --method: Point sampling method "poisson" (default), "poisson_average", "random"
--attributes: Attributes in the output file
--threads: Number of threads to use
```
