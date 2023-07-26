


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

	if (args.has("help")) {
		cout << "PotreeConverter <source> -o <outdir>" << endl;
		cout << endl << args.usage() << endl;
		exit(0);
	}

    if (!args.has("header-dir")) {
        cout << "No headers directory specified" << endl;
        cout << "PotreeConverter  -o <outdir> --head-dir <header-dir>" << endl;
        cout << endl << "For a list of options, use --help or -h" << endl;

        exit(1);
    }

    string headerDir = args.get("header-dir").as<string>();

    if (!fs::is_directory(headerDir)) {

        cout << "header-dir is not a directory" << endl;
        cout << "PotreeConverter -i <input data directory> -o <outdir> --head-dir <header-dir>" << endl;
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
        cout << "PotreeConverter  -o <outdir> --head-dir <header-dir>" << endl;
        cout << endl << "For a list of options, use --help or -h" << endl;

        exit(1);

	}

	outdir = fs::weakly_canonical(fs::path(outdir)).string();
    headerDir = fs::weakly_canonical(fs::path(headerDir)).string();
	//vector<string> flags = args.get("flags").as<vector<string>>();

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

	Options options;
	options.outdir = outdir;
    options.headerDir = headerDir;
	options.method = method;
	options.encoding = encoding;
	options.chunkMethod = chunkMethod;
	//options.flags = flags;
	options.attributes = attributes;
	options.generatePage = generatePage;
	options.pageName = pageName;
	options.pageTitle = pageTitle;
	options.projection = projection;

	options.keepChunks = keepChunks;
	options.noChunking = noChunking;
	options.noIndexing = noIndexing;

	//cout << "flags: ";
	//for (string flag : options.flags) {
	//	cout << flag << ", ";
	//}
	//cout << endl;

	return options;
}

/*struct Curated{
	string name;
	vector<Source> files;
};
Curated curateSources(vector<string> paths) {


	cout << "#paths: " << paths.size() << endl;

	vector<Source> sources;
	sources.reserve(paths.size());

	mutex mtx;
	auto parallel = std::execution::par;
	for_each(parallel, paths.begin(), paths.end(), [&mtx, &sources](string path) {

		auto header = loadLasHeader(path);
		//auto filesize = fs::file_size(path);

		Vector3 min = { header.min.x, header.min.y, header.min.z };
		Vector3 max = { header.max.x, header.max.y, header.max.z };

		Source source;
		source.path = path;
		source.min = min;
		source.max = max;
		source.numPoints = header.numPoints;
		source.filesize = header.numPoints * header.pointDataRecordLength;

		lock_guard<mutex> lock(mtx);
		sources.push_back(source);
	});

	return sources;
}*/



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
    /*Vector3 min = { Infinity , Infinity , Infinity };
    Vector3 max = { -Infinity , -Infinity , -Infinity };

    for(auto headerFile : headerFiles){
        auto header = loadLasHeader(headerFile);
        min.x = std::min(min.x, header.min.x);
        min.y = std::min(min.y, header.min.y);
        min.z = std::min(min.z, header.min.z);

        max.x = std::max(max.x, header.max.x);
        max.y = std::max(max.y, header.max.y);
        max.z = std::max(max.z, header.max.z);

        Vector3 min = { header.min.x, header.min.y, header.min.z };
        Vector3 max = { header.max.x, header.max.y, header.max.z };

        Source<SourceFileType::HEADER> source;
        source.path = headerFile;
        source.min = min;
        source.max = max;
        source.numPoints = header.numPoints;
        source.filesize = header.headerSize;

        headers.push_back(source);
    }

    return {min, max};*/

}
Stats computeStats(vector<Source> headers) {

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


	double cubeSize = (max - min).max();
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

// struct Monitor {
// 	thread t;
// 	bool stopRequested = false;

// 	void stop() {

// 		stopRequested = true;

// 		t.join();
// 	}
// };

// shared_ptr<Monitor> startMonitoring(State& state) {

// 	shared_ptr<Monitor> monitor = make_shared<Monitor>();

// 	monitor->t = thread([monitor, &state]() {

// 		using namespace std::chrono_literals;

// 		std::this_thread::sleep_for(1'000ms);

// 		while (!monitor->stopRequested) {

// 			auto ram = getMemoryData();
// 			auto CPU = getCpuData();
// 			double GB = 1024.0 * 1024.0 * 1024.0;

// 			double throughput = (double(state.pointsProcessed) / state.duration) / 1'000'000.0;

// 			double progressPass = 100.0 * state.progress();
// 			double progressTotal = (100.0 * double(state.currentPass - 1) + progressPass) / double(state.numPasses);

// 			string strProgressPass = formatNumber(progressPass) + "%";
// 			string strProgressTotal = formatNumber(progressTotal) + "%";
// 			string strTime = formatNumber(now()) + "s";
// 			string strDuration = formatNumber(state.duration) + "s";
// 			string strThroughput = formatNumber(throughput) + "MPs";

// 			string strRAM = formatNumber(double(ram.virtual_usedByProcess) / GB, 1)
// 				+ "GB (highest " + formatNumber(double(ram.virtual_usedByProcess_max) / GB, 1) + "GB)";
// 			string strCPU = formatNumber(CPU.usage) + "%";

// 			stringstream ss;
// 			ss << "[" << strProgressTotal << ", " << strTime << "], "
// 				<< "[" << state.name << ": " << strProgressPass << ", duration: " << strDuration << ", throughput: " << strThroughput << "]"
// 				<< "[RAM: " << strRAM << ", CPU: " << strCPU << "]";

// 			cout << ss.str() << endl;

// 			std::this_thread::sleep_for(1'000ms);
// 		}

// 	});

// 	return monitor;
// }
shared_ptr<indexer::Chunks> indexing(Options& options, string targetDir, State& state, indexer::Indexer& indexer, bool islastbatch) {

    if (options.noIndexing) {
        return nullptr;
    }

    /*if(task_num == MASTER) {

    }
    string wait = "waiting";
    MPI_Send(wait.c_str(), wait.length(),MPI_CHAR, MASTER, 0, MPI_COMM_WORLD);*/
    shared_ptr<indexer::Chunks> chunks;
    if (options.method == "random") {

        SamplerRandom sampler;
        chunks = indexer::doIndexing(targetDir, state, options, sampler, indexer, islastbatch);

    } else if (options.method == "poisson") {

        SamplerPoisson sampler;
        chunks = indexer::doIndexing(targetDir, state, options, sampler, indexer, islastbatch);

    } else if (options.method == "poisson_average") {

        SamplerPoissonAverage sampler;
        chunks = indexer::doIndexing(targetDir, state, options, sampler, indexer, islastbatch);

    }
    return chunks;
}

void finalMerge(Options& options, string targetDir, State& state, indexer::Indexer& indexer, shared_ptr<indexer::Chunks> chunks) {


    /*if(task_num == MASTER) {

    }
    string wait = "waiting";
    MPI_Send(wait.c_str(), wait.length(),MPI_CHAR, MASTER, 0, MPI_COMM_WORLD);*/
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
void process(Options& options, Stats& stats, State& state, string targetDir, Attributes outputAttributes, Monitor* monitor) {

    chunker_countsort_laszip::NodeLUT lut;
        if (options.noChunking) {
            return;
        }
        // if is always executed
        if (options.chunkMethod == "LASZIP") {

            if (task_id == MASTER) {
                lut = chunker_countsort_laszip::doCounting(stats.min, stats.max, state, targetDir, outputAttributes,
                                                           monitor);
            }

        } else if (options.chunkMethod == "LAS_CUSTOM") {

            //chunker_countsort::doChunking(sources[0].path, targetDir, state);

        } else if (options.chunkMethod == "SKIP") {

            // skip chunking

        } else {

            cout << "ERROR: unkown chunk method: " << options.chunkMethod << endl;
            exit(123);

        }
    MPI_Barrier(MPI_COMM_WORLD);

    int batchNum = 1;
    bool isLastbatch = false;
    indexer::Indexer indexer(targetDir);
    shared_ptr<indexer::Chunks> chunks;
    if(task_id == MASTER)RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Total indexing and distribution time including copy wait time")
    if(task_id == MASTER) RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Indexing and distribution copy wait time")
    while (!fs::exists(fs::path(targetDir + "/.indexing_copy_done_signals/batchno_" + to_string(batchNum) + "_copied"))) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if(task_id == MASTER) RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Indexing and distribution copy wait time")
    auto duration = 0.0;
    while (!isLastbatch) {
        // cout << "waiting for file" << endl;
        fstream batchfiles;

        batchfiles.open(targetDir + "/.indexing_copy_done_signals/batchno_" + to_string(batchNum) + "_copied", ios::in);
        string lazFiles;
        getline(batchfiles, lazFiles);
        vector<string> lazFilesVec = splitString(" ", lazFiles);
        string lastbatch;
        getline(batchfiles, lastbatch);
        if (lastbatch == "lastbatch") {
            isLastbatch = true;
        }
        if (task_id == MASTER) {

            auto sources = curateSources(lazFilesVec);
            RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Distribution time");
            auto tStart = now();
            chunker_countsort_laszip::doDistribution(stats.min, stats.max, state, lut, targetDir, sources,
                                                     outputAttributes, monitor);
            duration += now() - tStart;
             RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Distribution time");

    }
        if(task_id == MASTER)RECORD_TIMINGS_START (recordTimings::Machine::cpu, "wait time for concatenating octree files");
        while (!fs::exists(fs::path(targetDir + "/.indexing_copy_done_signals/batchno_" + to_string(batchNum-1) + "_concatenated")) && ((batchNum - 1) > 0)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        if (task_id == MASTER) RECORD_TIMINGS_STOP (recordTimings::Machine::cpu, "wait time for concatenating octree files");
        MPI_Barrier(MPI_COMM_WORLD);
        if(task_id == MASTER) RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Indexing time");
        auto tStart = now();
        chunks = indexing(options, targetDir, state, indexer, isLastbatch);
        duration += now() - tStart;
        if(task_id == MASTER) RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Indexing time");
        MPI_Barrier(MPI_COMM_WORLD);
        if(task_id == MASTER) {
            if (!isLastbatch) {
                fstream signalToCopier;
                signalToCopier.open(targetDir + "/.indexing_done_signals/batchno_" + to_string(batchNum) + "_indexed", ios::out);
                signalToCopier << to_string(duration);
                signalToCopier.close();
            }
            batchfiles.close();
            fs::remove(fs::path(targetDir + "/.indexing_copy_done_signals/batchno_" + to_string(batchNum) + "_copied"));
        }
        batchNum++;
        if(!isLastbatch)    duration = 0.0;
        if(task_id == MASTER) RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Indexing and distribution copy wait time")
        while (!fs::exists(fs::path(targetDir + "/.indexing_copy_done_signals/batchno_" + to_string(batchNum) + "_copied"))) {
            if (isLastbatch)
                break;
            else
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        MPI_Barrier(MPI_COMM_WORLD);
        if(task_id == MASTER)RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Indexing and distribution copy wait time")

    }

    if(task_id == MASTER)RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Total indexing and distribution time including copy wait time")

    if(task_id == MASTER) {
        RECORD_TIMINGS_START(recordTimings::Machine::cpu, "Final merge time");
        finalMerge(options, targetDir, state, indexer, chunks);
        fstream signalToCopier;
        signalToCopier.open(targetDir + "/.indexing_done_signals/batchno_" + to_string(batchNum - 1) + "_indexed", ios::out);
        signalToCopier << to_string(duration);
        signalToCopier.close();
        RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "Final merge time");

    }

    MPI_Barrier(MPI_COMM_WORLD);

}

/*void MPIchunkDistributor(string targetDir){

    thread t([targetDir]() {

        vector<string> allChunkFiles;
        for (const auto& entry : fs::directory_iterator(targetDir)) {
            string filename = entry.path().filename().string();
            allChunkFiles.push_back(filename);
        }
        int done = 0;
        while (done < allChunkFiles.size()){
        }

    });
    t.detach();
}*/



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

RECORD_TIMINGS_INIT();

//map<pid_t, recordTimings::Record_timings> thread_time_record_map;\
//map<string, vector<pid_t>> desc_thread_map;

int n_tasks, task_id;

int main(int argc, char **argv) {

    //RECORD_TIMINGS_DISABLE();
    cout << "PotreeConverterMPI started" << endl;
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &provided);
    if (provided != MPI_THREAD_SERIALIZED) {
        cout << "MPI does not support MPI_THREAD_SERIALIZED" << endl;
        exit(1);
    }


    MPI_Comm_size(MPI_COMM_WORLD, &n_tasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &task_id);
    // { // DEBUG STUFF

    // 	string hierarchyDir = "D:/dev/pointclouds/Riegl/retz_converted/.hierarchyChunks";
    // 	int hierarchyStepSize = 4;

    // 	HierarchyBuilder builder(hierarchyDir, hierarchyStepSize);
    // 	builder.build();

    // 	return 0;
    // }



    if(task_id == MASTER) RECORD_TIMINGS_START(recordTimings::Machine::cpu, "The total_ execution time");

    double tStart = now();

    auto exePath = fs::canonical(fs::absolute(argv[0])).parent_path().string();

    launchMemoryChecker(2 * 1024, 0.1);
    auto cpuData = getCpuData();

    cout << "#threads: " << cpuData.numProcessors << endl;

    auto options = parseArguments(argc, argv);



    auto headers = curateHeaders(options.headerDir);

    auto outputAttributes = computeOutputAttributes(headers, options.attributes);

    cout << toString(outputAttributes);

    auto stats = computeStats(headers);

    options.name = splitString("/", options.headerDir).back();

    string targetDir = options.outdir;
    if (options.generatePage && task_id == MASTER) {

        string pagedir = targetDir;
        generatePage(exePath, pagedir, options.pageName);

        targetDir = targetDir + "/pointclouds/" + options.pageName;
    }
    cout << "target directory: '" << targetDir << "'" << endl;

    string logFile = targetDir + "/log.txt";
    if (task_id == MASTER) {
        //fs::remove_all(targetDir);
        //fs::create_directories(targetDir);
        fs::create_directories(targetDir + "/chunks");
        fs::create_directories(targetDir + "/.hierarchyChunks");
        logger::addOutputFile(logFile);
    }
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




    if (task_id == MASTER) {
        createReport(options, headers, targetDir, stats, state, tStart);
    }




    if(task_id == MASTER) RECORD_TIMINGS_STOP(recordTimings::Machine::cpu, "The total_ execution time");


    if(task_id == MASTER) RECORD_TIMINGS_PRINT(cout);

MPI_Finalize();

return 0;
}