# About
This is a fork of the original PotreeConverter 2.0. The original repository can be found [here](https://github.com/potree/PotreeConverter).

The main branch contains the MPI implementation  of the PotreeConverter. The storage requirement for PotreeConverter is:
```angular2html
input_data_size + temporary_data_size + output_data_size
temporary_data_size = input_data_size * LAZ_compression_ratio
```
The LAZ compression ratio can be as large as 10. In most cases

```angular2html
output_data_size = 2 * input_data_size
```

Therefore, the strorage requirement for the PotreeConverter is about 13 times the input data size. THe MPI implementation of the PotreeConverter reduces the storage requirement by partitioning the input data and processing the partitions. The partitions are merged to generate the final output. The storag requirement for the MPI implementation is
13 times the size of the largest partition.



### Building PotreeConverterMPI

The tool can be built using the following commands:

```bash
git clone
cd PotreeConverter
git chechout mpi_partitioned_input
mkdir build
cd build
cmake ..
make
```

The tool is built in the `build` directory. The executable is named `PotreeConverterMPI`.

### Running PotreeConverterMPI

The tools is designed to run on a clsuter with SLURM or PBS scheduler. The cluster has a login node to submit jobs and multiple compute nodes. Run the follwoing command on the login node in the root directory of the repository:

```bash
python3 PotreeConverterPartitioned/run_PotreeConverterBatch.py <path to INI configuration file>
```

An format of theINI configuration file is provided below. The comments in the configuration file explain the different options.


```ini

[INPUT_OUTPUT]
#The input directory type. remote: the input directory is on a remote file system not accessible to the compute nodes and input files are copied to a temporary directory. local: the input directory is on a local file system accessible to the compute nodes and input files are not copied to a temporary directory.
InputDirType = local | remote
#The output directory type. remote: the output directory is on a remote file system not accessible to the compute nodes. local: the output directory is on a local file system accessible to the compute nodes
OutputDirType = local | remote
#The input directory containing the laz files.
InputDir = /path/to/input/directory
#The output directory for the converted data. The directory is created if it does not exist.
OutputDir = /path/to/output/directory
#The directory that contains the headers for the laz files. This is used to determine the bounding box of the input data. The name of the headers files should be same as the laz files and must have ".json" extension.
LazHeadersDir = /path/to/laz/headers/directory

[COPIER]
#The type of the copier. cp: uses cp to copy files. Only cp is supported at the moment.
Type = cp


[PARTITIONS]
#The file that contains the partition ins CSV format. The file should have the following columns: "filename" and "partition_id". The "filename" column should contain the name of the laz file and the "partition_id" column should contain the partition number of the laz file. The partition_id should be a number between 0 and the number of partitions - 1. For un-partitioned data, all the files should have the same partition_id = 0. A example file is provided in the ""PotreeConverterPartitioned/partitions/ahn3_partitions_8x8.csv"
CSV = /path/to/partitions/csv/file

[TMP_STORAGE]
#The maximum temporary space available in bytes. Use python expression format For example 600*(1024)*3
MaxTmpSpaceAvailable = 600*(1024)*3
#The directory to store the temporary files. The directory is created if it does not exist.
TmpDir = /path/to/tmp/directory

#The amount copied from the input directory to the temporary directory in bytes for the the counting phase of the converter. Use python expression format. For example 20*(1024)**3
CountingBatchSize = 20*(1024)**3
#The amount copied from the input directory to the temporary directory in bytes for the the distribution phase of the converter. Use python expression format.
DistributionBatchSize = 20*(1024)**3

#The expected compression ratio of the input data. This is used to estimate the required disk space for the temporary and output files.
LazCompressionRatio = 7

[PROGRAM]
#The path to the converter program.
Path = /path/to/PotreeConverterMPI
#The options to pass to the converter program. See below for different options
Options = <converter options>

[SCHEDULER]
#The scheduler type. slurm: SLURM scheduler. pbs: PBS scheduler. local: program is run locally.
Type = sbatch | pbs | local
#The parameters to pass to the scheduler. See the scheduler documentation for the parameters.
Parameters = <parameters>
```
Different program options can be passed to the converter program using the `ConverterOptions` parameter. The following options are supported:

```
--encoding: Encoding type "BROTLI", "UNCOMPRESSED"(default)
-m, --method: Point sampling method "poisson"(default), "poisson_average", "random"
--attributes: Attributes in the output file. If not specified, all attributes are included in the output file.
--threads: Number of threads to use
--bounds: Bounds of the pointcloud to be converted. The format shoud be: [minx,maxx],[miny,maxy],[minz,maxz]. If not provided, the bounds will be computed from the input files.
--max-mem: Maximum memory to be used by the program in GB. If not provided, the program will use all the available memory.
```

