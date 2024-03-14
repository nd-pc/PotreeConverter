
# About

PotreeConverterMPI generates an octree LOD structure for streaming and real-time rendering of massive point clouds. The results can be viewed in web browsers with [Potree](https://github.com/potree/potree) or as a desktop application with [PotreeDesktop](https://github.com/potree/PotreeDesktop). 

Version 2.0 is a complete rewrite with following differences over the previous version 1.7:

* About 10 to 50 times faster than PotreeConverterMPI 1.7 on SSDs, when executed on a single machine.
* Produces a total of 3 files instead of thousands to tens of millions of files. The reduction of the number of files improves file system operations such as copy, delete and upload to servers from hours and days to seconds and minutes. 
* Better support for standard LAS attributes and arbitrary extra attributes. Full support (e.g. int64 and uint64) in development.
* Optional compression is not yet available in the new converter but on the roadmap for a future update.
* Open MPI implementation for execution on multi-machine systems, designed to run on a cluster with SLURM or PBS scheduler.

Altough the converter made a major step to version 2.0, the format it produces is also supported by Potree 1.7. The Potree viewer is scheduled to make the major step to version 2.0 in 2021, with a rewrite in WebGPU. 


# Publications

* [Potree: Rendering Large Point Clouds in Web Browsers](https://www.cg.tuwien.ac.at/research/publications/2016/SCHUETZ-2016-POT/SCHUETZ-2016-POT-thesis.pdf)
* [Fast Out-of-Core Octree Generation for Massive Point Clouds](https://www.cg.tuwien.ac.at/research/publications/2020/SCHUETZ-2020-MPC/), _Schütz M., Ohrhallinger S., Wimmer M._


# Dependencies

Running PotreeConverterMPI has the following dependencies:
- Python	(3.9.12 or later)
- PDAL		(2.4.3 or later)

Building PotreeConverterMPI has the following dependencies:
* CMake		(3.16 or later)
* g++		(9.4 or later)
* openmpi	(5.0.1 or later)
* psutil	(5.9.8 or later)
* tbb		(2021.7.0 or later)
* threads	()

Note, this program is tested with OpenMPI 4.1.1 which implements the MPI 3.1 standard. It may work with other MPI implementations that support the MPI 3.1 standard, but this is not guaranteed.


# Getting Started

1. Download windows binaries or
    * Download source code
	* Install [CMake](https://cmake.org/) 3.16 or later
	* Create and jump into folder "build"
	    ```
	    mkdir build
	    cd build
	    ```
	* run 
	    ```
	    cmake ../
	    ```
	* On linux, run: ```make```
	* On windows, open Visual Studio 2019 Project ./Converter/Converter.sln and compile it in release mode
# 2. Run ```PotreeConverter.exe <input> -o <outputDir>```
#     * Optionally specify the sampling strategy:
# 	  * Poisson-disk sampling (default): ```PotreeConverter.exe <input> -o <outputDir> -m poisson```
# 	  * Random sampling: ```PotreeConverter.exe <input> -o <outputDir> -m random```
2. Run ```util_scripts/create_json_headers.sh /path/to/input/laz/directory /path/to/laz/headers/directory```
3. Configure program parameters in ini file. A template is provided in examples/mpi.ini
4. Run ```python3 run_PotreeConverterBatched.py <path/to/ini/configuration/file>```
	* Either run locally, or on a cluster with SLURM or PBS scheduler. The parallel process should be run on the login node.
