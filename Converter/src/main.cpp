


#include <iostream>
#include <execution>
#include "MPIcommon.h"
#include "unsuck/unsuck.hpp"
#include "chunker_countsort_laszip.h"
#include "indexer.h"
#include "sampler_poisson.h"
#include "sampler_poisson_average.h"
#include "sampler_random.h"
#include "logger.h"

#include "arguments/Arguments.hpp"

#include "record_timings.hpp"


using json = nlohmann::json;



using namespace std;

Options parseArguments(int argc, char** argv) {
	Arguments args(argc, argv);


    args.addArgument("header-dir", "LAS/LAZ headers directory");
	args.addArgument("help,h", "Display help information");
	args.addArgument("outdir,o", "Output directory");
	args.addArgument("encoding", "Encoding type \"BROTLI\", \"UNCOMPRESSED\" (default)");
	args.addArgument("method,m", "Point sampling method \"poisson\", \"poisson_average\", \"random\"");
	args.addArgument("chunkMethod", "Chunking method");
	args.addArgument("keep-chunks", "Skip deleting temporary chunks during conversion");
	args.addArgument("no-chunking", "Disable chunking phase");
	args.addArgument("no-indexing", "Disable indexing phase");
	args.addArgument("attributes", "Attributes in output file");
	args.addArgument("projection", "Add the projection of the pointcloud to the metadata");
	args.addArgument("generate-page,p", "Generate a ready to use web page with the given name");
	args.addArgument("title", "Page title used when generating a web page");
    args.addArgument("threads", "Number of threads to use");
    args.addArgument("bounds", "Bounds of the pointcloud to be converted. The format shoud be: [minx,maxx],[miny,maxy],[minz,maxz]. If not provided, the bounds will be computed from the input files.");
    args.addArgument("max-mem", "Maximum memory to be used by the program in GB. If not provided, the program will use all the available memory.");

	if (args.has("help")) {
		cout << "PotreeConverter <source> -o <outdir>" << endl;
		cout << endl << args.usage() << endl;
		exit(0);
	}

    if (!args.has("header-dir")) {
        cout << "No headers directory specified" << endl;
        cout << "PotreeConverterMPI  -o <outdir> --header-dir <header-dir>" << endl;
        cout << endl << "For a list of options, use --help or -h" << endl;

        exit(1);
    }

    string headerDir = args.get("header-dir").as<string>();

    if (!fs::is_directory(headerDir)) {

        cout << "header-dir is not a directory" << endl;
        cout << "PotreeConverterMPI -i <input data directory> -o <outdir> --head-dir <header-dir>" << endl;
        cout << endl << "For a list of options, use --help or -h" << endl;

        exit(1);
    }
	string encoding = args.get("encoding").as<string>("DEFAULT");
	string method = args.get("method").as<string>("poisson");
	string chunkMethod = args.get("chunkMethod").as<string>("LASZIP");

	string outdir = "";
	if (args.has("outdir")) {
		outdir = args.get("outdir").as<string>();
	} else {
        cout << "No output directory specified" << endl;
        cout << "PotreeConverterMPI  -o <outdir> --header-dir <header-dir>" << endl;
        cout << endl << "For a list of options, use --help or -h" << endl;

        exit(1);

	}

	outdir = fs::weakly_canonical(fs::path(outdir)).string();
    headerDir = fs::weakly_canonical(fs::path(headerDir)).string();

	vector<string> attributes = args.get("attributes").as<vector<string>>();

	bool generatePage = args.has("generate-page");
	string pageName = "";
	if (generatePage) {
		pageName = args.get("generate-page").as<string>();
	}
	string pageTitle = args.get("title").as<string>();
	string projection = args.get("projection").as<string>();

	bool keepChunks = args.has("keep-chunks");
	bool noChunking = args.has("no-chunking");
	bool noIndexing = args.has("no-indexing");
    bool boundsProvided = args.has("bounds");


	Options options;
	options.outdir = outdir;
    options.headerDir = headerDir;
	options.method = method;
	options.encoding = encoding;
	options.chunkMethod = chunkMethod;
	options.attributes = attributes;
	options.generatePage = generatePage;
	options.pageName = pageName;
	options.pageTitle = pageTitle;
	options.projection = projection;
	options.keepChunks = keepChunks;
	options.noChunking = noChunking;
	options.noIndexing = noIndexing;
    if (boundsProvided) {
        options.manualBounds = args.get("bounds").as<string>();
        std::regex pattern("\\[(-?\\d+\\.\\d+),(-?\\d+\\.\\d+)\\],\\[(-?\\d+\\.\\d+),(-?\\d+\\.\\d+)\\],\\[(-?\\d+\\.\\d+),(-?\\d+\\.\\d+)\\]");
        std::smatch matches;
        if (!std::regex_match( options.manualBounds, matches, pattern)) {
            cout << "bounds format is not correct. The format should be: [minx,maxx],[miny,maxy],[minz,maxz]" << endl;
            exit(1);
        }
    }
    int maxThreads = (int)std::thread::hardware_concurrency();
    int threads = args.get("threads").as<int>(maxThreads);
    setNumProcessors(threads);
    options.memoryBudget = args.get("max-mem").as<int>(getMemoryData().physical_total/(1024 * 1024 * 1024));




	return options;
}





struct Stats {
	Vector3 min = { Infinity , Infinity , Infinity };
	Vector3 max = { -Infinity , -Infinity , -Infinity };
	int64_t totalBytes = 0;
	int64_t totalPoints = 0;
};
struct MinMax {
    Vector3 min = { Infinity , Infinity , Infinity };
    Vector3 max = { -Infinity , -Infinity , -Infinity };
};


vector<Source> curateHeaders(string headerDir) {


    vector<string> headerFiles;
    for (auto &entry: fs::directory_iterator(headerDir)) {
                string str = entry.path().string();
                if (iEndsWith(str, "json")) {
                    headerFiles.push_back(str);
                }
    }

    cout << "#paths: " << headerFiles.size() << endl;


    vector<Source> sources;
    //sources.reserve(headerFiles.size());

    mutex mtx;
    auto parallel = std::execution::par;
    for_each(parallel, headerFiles.begin(), headerFiles.end(), [&mtx, &sources](string path) {

        string headerText = readTextFile(path);
        json js = json::parse(headerText);

        Vector3 min = {
                js["metadata"]["minx"].get<double>(),
                js["metadata"]["miny"].get<double>(),
                js["metadata"]["minz"].get<double>()
        };


        Vector3 max = {
                js["metadata"]["maxx"].get<double>(),
                js["metadata"]["maxy"].get<double>(),
                js["metadata"]["maxz"].get<double>()
        };

        Vector3 scale = {
                js["metadata"]["scale_x"].get<double>(),
                js["metadata"]["scale_y"].get<double>(),
                js["metadata"]["scale_z"].get<double>()
        };


        Source source;
        source.path = path;
        source.min = min;
        source.max = max;
        source.scale = scale;
        source.numPoints =  js["metadata"]["count"].get<uint64_t>();
        source.pointDataFormat =  js["metadata"]["dataformat_id"].get<uint8_t>();
        source.pointDataRecordLength =  js["metadata"]["point_length"].get<uint16_t>();
        source.filesize = source.numPoints * source.pointDataRecordLength;
        int n = 0;
        while(js["metadata"].contains("vlr_"+ to_string(n))){
            vlr v;
            string str =  js["metadata"]["vlr_"+ to_string(n)]["data"].get<string>();
            for (char c : str) {
                v.data.push_back((uint8_t)c);
            }
            v.recordID =  js["metadata"]["vlr_"+ to_string(n)]["record_id"].get<uint16_t>();
            source.vlrs.push_back(v);
            n++;
        }
        source.type = SourceFileType::HEADER;


        lock_guard<mutex> lock(mtx);

        sources.push_back(source);
    });


    return std::move(sources);


}


Stats computeStats(vector<Source> headers, string boundString) {

	Vector3 min = { Infinity , Infinity , Infinity };
	Vector3 max = { -Infinity , -Infinity , -Infinity };

	int64_t totalBytes = 0;
	int64_t totalPoints = 0;

	for(auto source : headers){

        min.x = std::min(min.x, source.min.x);
        min.y = std::min(min.y, source.min.y);
        min.z = std::min(min.z, source.min.z);

        max.x = std::max(max.x, source.max.x);
        max.y = std::max(max.y, source.max.y);
        max.z = std::max(max.z, source.max.z);

		totalPoints += source.numPoints;
		totalBytes += source.numPoints * source.pointDataRecordLength;
	}
    if (!boundString.empty()) {
        parseBoundString(boundString, min, max);
    }
	double cubeSize = ceil((max - min).max());
	Vector3 size = { cubeSize, cubeSize, cubeSize };
	max = min + cubeSize;

	string strMin = "[" + to_string(min.x) + ", " + to_string(min.y) + ", " + to_string(min.z) + "]";
	string strMax = "[" + to_string(max.x) + ", " + to_string(max.y) + ", " + to_string(max.z) + "]";
	string strSize = "[" + to_string(size.x) + ", " + to_string(size.y) + ", " + to_string(size.z) + "]";

	string strTotalFileSize;
	{
		int64_t KB = 1024;
		int64_t MB = 1024 * KB;
		int64_t GB = 1024 * MB;
		int64_t TB = 1024 * GB;

		if (totalBytes >= TB) {
			strTotalFileSize = formatNumber(double(totalBytes) / double(TB), 1) + " TB";
		} else if (totalBytes >= GB) {
			strTotalFileSize = formatNumber(double(totalBytes) / double(GB), 1) + " GB";
		} else if (totalBytes >= MB) {
			strTotalFileSize = formatNumber(double(totalBytes) / double(MB), 1) + " MB";
		} else {
			strTotalFileSize = formatNumber(double(totalBytes), 1) + " bytes";
		}
	}
	

	cout << "cubicAABB: {\n";
	cout << "	\"min\": " << strMin << ",\n";
	cout << "	\"max\": " << strMax << ",\n";
	cout << "	\"size\": " << strSize << "\n";
	cout << "}\n";

	cout << "#points: " << formatNumber(totalPoints) << endl;
	cout << "total file size: " << strTotalFileSize << endl;

	{ // sanity check
		bool sizeError = (size.x == 0.0) || (size.y == 0.0) || (size.z == 0);
		if (sizeError) {
			logger::ERROR("invalid bounding box. at least one axis has a size of zero.");

			exit(123);
		}
		
	}

	return { min, max, totalBytes, totalPoints };
}


shared_ptr<indexer::Chunks> indexing(Options& options, string chunksDir, State& state, indexer::Indexer& indexer, bool islastbatch, shared_ptr<map<string, vector<string>>> chunkFiletoPathMap, int batchNum) {

    if (options.noIndexing) {
        return nullptr;
    }


    if (options.method == "random") {

        SamplerRandom sampler;
        return indexer::doIndexing(chunksDir, state, options, sampler, indexer, islastbatch, chunkFiletoPathMap, batchNum);

    } else if (options.method == "poisson") {

        SamplerPoisson sampler;
        return indexer::doIndexing(chunksDir, state, options, sampler, indexer, islastbatch, chunkFiletoPathMap, batchNum);

    } else if (options.method == "poisson_average") {

        SamplerPoissonAverage sampler;
        return indexer::doIndexing(chunksDir, state, options, sampler, indexer, islastbatch, chunkFiletoPathMap, batchNum);

    }
    else {
        cout << "ERROR: unkown sampling method: " << options.method << endl;
        exit(123);
    }
}

void finalMerge(Options& options, string targetDir, State& state, indexer::Indexer& indexer, shared_ptr<indexer::Chunks> chunks) {


    if (options.method == "random") {

        SamplerRandom sampler;
        doFinalMerge(indexer, chunks, targetDir, sampler, options, state);

    } else if (options.method == "poisson") {

        SamplerPoisson sampler;
        doFinalMerge(indexer, chunks, targetDir, sampler, options, state);

    } else if (options.method == "poisson_average") {

        SamplerPoissonAverage sampler;
        doFinalMerge(indexer, chunks, targetDir, sampler, options, state);

    }
}

shared_ptr<map<string, vector<string>>> mapChunkstoPaths(string targetDir){
    // Traverse through the "chunk_<process_id>" directories and create a map of chunk file names to the paths of the chunk files.
    shared_ptr<map<string, vector<string>>> chunkFiletoPathMap = make_shared<map<string, vector<string>>>();
    for (int i = 0; i < n_processes; i++) {
        for (auto& entry : fs::directory_iterator(targetDir + "/" + "chunks_" + to_string(i))) {
            auto filepath = entry.path();
            if (iEndsWith(filepath.string(), ".bin")) {
                (*chunkFiletoPathMap)[filepath.filename().string()].push_back(filepath.string());
            }
        }
    }
    return chunkFiletoPathMap;


}
void process(Options& options, Stats& stats, State& state, string targetDir, Attributes &outputAttributes, Monitor* monitor) {

    chunker_countsort_laszip::NodeLUT lut;
    if (options.noChunking) {
        return;
    }
    // if is always executed
    if (options.chunkMethod == "LASZIP") {

        lut = chunker_countsort_laszip::doCounting(stats.min, stats.max, state, targetDir, outputAttributes,
                                                       monitor);

    } else if (options.chunkMethod == "LAS_CUSTOM") {

        //chunker_countsort::doChunking(sources[0].path, targetDir, state);

    } else if (options.chunkMethod == "SKIP") {

        // skip chunking

    } else {

        cout << "ERROR: unkown chunk method: " << options.chunkMethod << endl;
        exit(123);

    }

    int batchNum = 0;
    bool isLastbatch = false;
    indexer::Indexer indexer(targetDir);
    indexer.root = make_shared<Node>("r", stats.min, stats.max);

    shared_ptr<indexer::Chunks> chunks = nullptr;
    MPI_Barrier(MPI_COMM_WORLD);
    if(process_id == ROOT)RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Total indexing and distribution time including copy wait time")
    auto indexDuration = 0.0;
    int miniBatchNum = 0;
    // Loop until all the batches are processed. In distribution + indexing a batch is a partition.
    // For distribution, a partition is subdivided into mini-batches to overlap copying and processing.
    while (!isLastbatch) {
        bool isLastminiBatchinPartition = false;

        auto distDuration = 0.0;
        RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Total distribution time including copy wait time")
        // Loop until all the mini-batches in a partition are processed.
        while (!isLastminiBatchinPartition){
            if(process_id == ROOT) RECORD_TIMINGS_START(recordTimings::Machine::cpu, "waiting for copying in distribution")
            // Wait for the copying of the mini-batch to be done.
            while (!fs::exists(fs::path(targetDir + "/distribution_copy_done_signals/batchno_" + to_string(miniBatchNum) + "_written"))) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            if(process_id == ROOT) RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "waiting for copying in distribution")
            // The batchno_miniBatchNum_copied file is written by the copier after copying the mini-batch. It contains the list of laz files in the mini-batch.
            // The second line in the file is "lastminibatchinpartition" if the mini-batch is the last mini-batch in the partition, otherwise it is "notlastminibatchinpartition".
            fstream batchfiles;
            batchfiles.open(targetDir + "/distribution_copy_done_signals/batchno_" + to_string(miniBatchNum) + "_copied", ios::in);
            string lazFiles;
            getline(batchfiles, lazFiles);
            vector<string> lazFilesVec = splitString(",", lazFiles);
            string lastbatch;
            string lastminibatchinpartition;
            getline(batchfiles, lastminibatchinpartition);
            if (lastminibatchinpartition == "lastminibatchinpartition") {
                isLastminiBatchinPartition = true;
            } else if (lastminibatchinpartition == "notlastminibatchinpartition") {
                isLastminiBatchinPartition = false;
            } else {
                cout << "ERROR: unkown batch type: " << lastminibatchinpartition << endl;
                exit(123);
            }
            getline(batchfiles, lastbatch);
            if (lastbatch == "lastbatch") {
                isLastbatch = true;
            } else if (lastbatch == "notlastbatch") {
                isLastbatch = false;
            } else {
                cout << "ERROR: unkown batch type: " << lastbatch << endl;
                exit(123);
            }
            batchfiles.close();
            auto sources = curateSources(lazFilesVec);
            MPI_Barrier(MPI_COMM_WORLD);
            if (process_id == ROOT) RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Distribution time");
            auto tStartDist = now();
            chunker_countsort_laszip::doDistribution(stats.min, stats.max, state, lut, targetDir + "/chunks_" + to_string(process_id), sources,
                                                     outputAttributes, monitor);


            MPI_Barrier(MPI_COMM_WORLD);
            distDuration += now() - tStartDist;

            if (process_id == ROOT)  RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Distribution time");
            // Write the distribution done signal file. It contains the duration of the distribution.
            if (process_id == ROOT) {
                fstream signalToCopier;
                signalToCopier.open(
                        targetDir + "/distribution_done_signals/batchno_" + to_string(miniBatchNum) + "_distribution_done",
                        ios::out);
                if (isLastminiBatchinPartition) {
                    signalToCopier << to_string(distDuration);
                }
                else {
                    signalToCopier << "-1.0";
                }
                signalToCopier.close();
                {
                    fstream().open(
                            targetDir + "/distribution_done_signals/batchno_" + to_string(miniBatchNum) + "_distribution" +
                            "_time_written",
                            ios::out);
                }
            }
            miniBatchNum++;

        }
        MPI_Barrier(MPI_COMM_WORLD);
        RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Total distribution time including copy wait time")
        // In distribution, it may be possible that  a chunk file may have been partially wriiten by mutliple processes.
        // Som traverse through the "chunk_<process_id>" directories and create a map of chunk file names to the paths of the chunk files.
        // In the indexing phase, the chunk file paths are used to read the chunk files and index them.
        // The files with the same name are treated as a single chunk file.
        shared_ptr<map<string, vector<string>>> chunkFiletoPathMap = mapChunkstoPaths(targetDir);
        MPI_Barrier(MPI_COMM_WORLD);

        if(process_id == ROOT)RECORD_TIMINGS_START (recordTimings::Machine::cpu, "wait time for concatenating octree files");
        // Beore indexing, wait until the output of the previous partitions is concatenated to octree.bin. We allow atmost output of two partitions to remain in the temporary storage.
        while (!fs::exists(fs::path(targetDir + "/indexing_copy_done_signals/batchno_" + to_string(batchNum-2) + "_concatenated")) && ((batchNum - 2) >= 0)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        if (process_id == ROOT) RECORD_TIMINGS_STOP (recordTimings::Machine::cpu, "wait time for concatenating octree files");
        MPI_Barrier(MPI_COMM_WORLD);

        if(process_id == ROOT) {
            RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Indexing time");
        }
        auto tStartIndex = now();
        chunks = indexing(options, targetDir + "/chunks_" + to_string(process_id), state, indexer, isLastbatch, chunkFiletoPathMap, batchNum);
        MPI_Barrier(MPI_COMM_WORLD);
        indexDuration = now() - tStartIndex;
        if(process_id == ROOT) RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Indexing time");


        if(process_id == ROOT) {

            if (!isLastbatch) {
                fstream signalToCopier;
                signalToCopier.open(targetDir + "/indexing_done_signals/batchno_" + to_string(batchNum) + "_indexing_done", ios::out);
                signalToCopier << to_string(indexDuration);// << "\n" << "msgcomplete";
                signalToCopier.close();
                {
                    fstream().open(
                            targetDir + "/indexing_done_signals/batchno_" + to_string(batchNum) + "_indexing"+ "_time_written",
                            ios::out);
                }
            }
        }
        batchNum++;
        MPI_Barrier(MPI_COMM_WORLD);

    }


    if(process_id == ROOT)RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Total indexing and distribution time including copy wait time")


    if(process_id == ROOT) {
        RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Final merge time");
        cout << "Final merge..." << endl;
        auto tStartFinalMerge = now();
        finalMerge(options, targetDir, state, indexer, chunks);
        auto finalMergeDuration = now() - tStartFinalMerge;
        cout << "Final merge done" << endl;
        fstream signalToCopier;
        signalToCopier.open(targetDir + "/indexing_done_signals/batchno_" + to_string(batchNum - 1) + "_indexing_done", ios::out);
        signalToCopier << to_string(indexDuration + finalMergeDuration);
        signalToCopier.close();
        {
            fstream().open(
                    targetDir + "/indexing_done_signals/batchno_" + to_string(batchNum - 1) + "_indexing"+ "_time_written",
                    ios::out);
        }
        RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Final merge time");
        RECORD_TIMINGS_START (recordTimings::Machine::cpu, "wait time for concatenating octree files");
        while (!fs::exists(fs::path(targetDir + "/indexing_copy_done_signals/batchno_" + to_string(batchNum-1) + "_concatenated")) && ((batchNum - 1) >= 0)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        RECORD_TIMINGS_STOP (recordTimings::Machine::cpu, "wait time for concatenating octree files");
    }

    MPI_Barrier(MPI_COMM_WORLD);

}




void createReport(Options& options, vector<Source> sources, string targetDir, Stats& stats, State& state, double tStart) {
	double duration = now() - tStart;
	double throughputMB = (stats.totalBytes / duration) / (1024 * 1024);
	double throughputP = (double(stats.totalPoints) / double(duration)) / 1'000'000.0;

	double kb = 1024.0;
	double mb = 1024.0 * 1024.0;
	double gb = 1024.0 * 1024.0 * 1024.0;
	double inputSize = 0;
	string inputSizeUnit = "";
	if (stats.totalBytes <= 10.0 * kb) {
		inputSize = stats.totalBytes / kb;
		inputSizeUnit = "KB";
	} else if (stats.totalBytes <= 10.0 * mb) {
		inputSize = stats.totalBytes / mb;
		inputSizeUnit = "MB";
	} else if (stats.totalBytes <= 10.0 * gb) {
		inputSize = stats.totalBytes / gb;
		inputSizeUnit = "GB";
	} else {
		inputSize = stats.totalBytes / gb;
		inputSizeUnit = "GB";
	}

	cout << endl;
	cout << "=======================================" << endl;
	cout << "=== STATS                              " << endl;
	cout << "=======================================" << endl;

	cout << "#points:               " << formatNumber(stats.totalPoints) << endl;
	cout << "#input files:          " << formatNumber(sources.size()) << endl;
	cout << "sampling method:       " << options.method << endl;
	cout << "chunk method:          " << options.chunkMethod << endl;
	cout << "input file size:       " << formatNumber(inputSize, 1) << inputSizeUnit << endl;
	cout << "duration:              " << formatNumber(duration, 3) << "s" << endl;
	cout << "throughput (MB/s)      " << formatNumber(throughputMB) << "MB" << endl;
	cout << "throughput (points/s)  " << formatNumber(throughputP, 1) << "M" << endl;
	cout << "output location:       " << targetDir << endl;

	

	for (auto [key, value] : state.values) {
		cout << key << ": \t" << value << endl;
	}


}

void generatePage(string exePath, string pagedir, string pagename) {
	string templateDir = exePath + "/resources/page_template";
	string templateSourcePath = templateDir + "/viewer_template.html";

	string pageTargetPath = pagedir + "/" + pagename + ".html";

	try{
		fs::copy(templateDir, pagedir, fs::copy_options::overwrite_existing | fs::copy_options::recursive);
	} catch (std::exception & e) {
		string msg = e.what();
		logger::ERROR(msg);
	}

	fs::remove(pagedir + "/viewer_template.html");

	{ // configure page template
		string strTemplate = readFile(templateSourcePath);

		string strPointcloudTemplate = 
		R"V0G0N(

		Potree.loadPointCloud("<!-- URL -->", "<!-- NAME -->", e => {
			let scene = viewer.scene;
			let pointcloud = e.pointcloud;
			
			let material = pointcloud.material;
			material.size = 1;
			material.pointSizeType = Potree.PointSizeType.ADAPTIVE;
			material.shape = Potree.PointShape.SQUARE;
			material.activeAttributeName = "rgba";
			
			scene.addPointCloud(pointcloud);
			
			viewer.fitToScreen();
		});

		)V0G0N";

		string url = "./pointclouds/" + pagename + "/metadata.json";

		string strPointcloud = stringReplace(strPointcloudTemplate, "<!-- URL -->", url);
		strPointcloud = stringReplace(strPointcloud, "<!-- NAME -->", pagename);

		string strPage = stringReplace(strTemplate, "<!-- INCLUDE POINTCLOUD -->", strPointcloud);


		writeFile(pageTargetPath, strPage);

	}

}

#include "HierarchyBuilder.h"



int n_processes, process_id;

RECORD_TIMINGS_INIT();
int main(int argc, char **argv) {


    //RECORD_TIMINGS_DISABLE();
    cout << "PotreeConverterMPI started" << endl;
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &provided);
    if (provided != MPI_THREAD_SERIALIZED) {
        cout << "MPI does not support MPI_THREAD_SERIALIZED" << endl;
        exit(1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &n_processes);
    MPI_Comm_rank(MPI_COMM_WORLD, &process_id);




    if(process_id == ROOT) RECORD_TIMINGS_START(recordTimings::Machine::cpu, "The total_execution time");

    double tStart = now();

    auto exePath = fs::canonical(fs::absolute(argv[0])).parent_path().string();

    launchMemoryChecker(2 * 1024, 0.1);




    auto options = parseArguments(argc, argv);

    auto cpuData = getCpuData();

    cout << "#threads: " << cpuData.numProcessors << endl;

    auto headers = curateHeaders(options.headerDir);

    auto outputAttributes = computeOutputAttributes(headers, options.attributes, options.manualBounds);

    cout << toString(outputAttributes);

    auto stats = computeStats(headers, options.manualBounds);

    options.name = splitString("/", options.headerDir).back();

    string targetDir = options.outdir;
    if (options.generatePage && process_id == ROOT) {

        string pagedir = targetDir;
        generatePage(exePath, pagedir, options.pageName);

        targetDir = targetDir + "/pointclouds/" + options.pageName;
    }
    cout << "target directory: '" << targetDir << "'" << endl;

    if (process_id == ROOT) fs::create_directories(targetDir + "/hierarchyChunks");
    fs::create_directories(targetDir + "/chunks_" + to_string(process_id));

    string logFile = targetDir + "/log_" + to_string(process_id) + ".txt";
    logger::addOutputFile(logFile);

    MPI_Barrier(MPI_COMM_WORLD);
    State state;
    state.pointsTotal = stats.totalPoints;
    state.bytesProcessed = stats.totalBytes;

    // auto monitor = startMonitoring(state);
    auto monitor = make_shared<Monitor>(&state);
    //monitor->start();

    // this is the real important stuff

    //output attributes are point attributes in LAZ including scale and offset
    //monitor for printing output messages about CPU, RAM usage and throughput
    //sate for keeping track of points processed
    process(options, stats, state, targetDir, outputAttributes, monitor.get());




    if (process_id == ROOT) {
        createReport(options, headers, targetDir, stats, state, tStart);
    }




    if(process_id == ROOT) RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "The total_execution time");

    fstream recordTimingsFile;
    recordTimingsFile.open(targetDir + "/recordTimings" + to_string(process_id) + ".txt", ios::out);
    RECORD_TIMINGS_PRINT(recordTimingsFile);

    MPI_Finalize();

    return 0;
}