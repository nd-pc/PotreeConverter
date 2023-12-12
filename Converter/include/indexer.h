
#pragma once

#include <string>
#include <filesystem>
#include <algorithm>
#include <deque>
#include <unordered_map>
#include <atomic>
#include <algorithm>
#include <functional>
#include <memory>
#include <fstream>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <map>

#include "json/json.hpp"

#include "Attributes.h"
#include "converter_utils.h"
#include "Vector3.h"
#include "unsuck/unsuck.hpp"
#include "unsuck/TaskPool.hpp"
#include "structures.h"

#include "MPIcommon.h"

using json = nlohmann::json;


using std::atomic_int64_t;
using std::atomic_int64_t;
using std::deque;
using std::string;
using std::unordered_map;
using std::function;
using std::shared_ptr;
using std::make_shared;
using std::fstream;
using std::mutex;

namespace fs = std::filesystem;

namespace indexer{

	//constexpr int numSampleThreads = 10;
	//constexpr int numFlushThreads = 36;
	constexpr int maxPointsPerChunk = 10'000;

	inline int numSampleThreads() {
		return getCpuData().numProcessors;
	}

	struct Hierarchy {
		int64_t stepSize = 0;
		vector<uint8_t> buffer;
		int64_t firstChunkSize = 0;
	};

	struct Chunk {
		Vector3 min;
		Vector3 max;

		vector<string> files;
		string id;
        //-----------------MPI---------------------
        int64_t numPoints = 0; // Total numPoints in the chunk
        //--------------MPI--------------------------
	};

	struct Chunks {
		vector<shared_ptr<Chunk>> list;
		Vector3 min;
		Vector3 max;
		Attributes attributes;

		Chunks(vector<shared_ptr<Chunk>> list, Vector3 min, Vector3 max) {
			this->list = list;
			this->min = min;
			this->max = max;
		}

	};

	shared_ptr<Chunks> getChunks(string pathIn);


	struct Indexer;

	struct Writer {

		Indexer* indexer = nullptr;
		int64_t capacity = 16 * 1024 * 1024;

		// copy node data here first
		shared_ptr<Buffer> activeBuffer = nullptr;

		// backlog of buffers that reached capacity and are ready to be written to disk
		deque<shared_ptr<Buffer>> backlog;

		bool closeRequested = false;
		bool closed = false;
		std::condition_variable cvClose;

		fstream fsOctree;

		//thread tWrite;

		mutex mtx;

		Writer(Indexer* indexer);

        void launch();

		void writeAndUnload(Node* node);

		void launchWriterThread();

		void closeAndWait();

		int64_t backlogSizeMB();

	};

	struct HierarchyFlusher{

		struct HNode{
			string name;
			int64_t byteOffset = 0;
			int64_t byteSize = 0;
			int64_t numPoints = 0;
		};

		mutex mtx;
		string path;
		//unordered_map<string, int> chunks;
		vector<HNode> buffer;

		HierarchyFlusher(string path){
			this->path = path;


			this->clear();
		}

		void clear(){
			fs::remove_all(path);

			fs::create_directories(path);
		}

		void write(Node* node, int hierarchyStepSize){
			lock_guard<mutex> lock(mtx);

			HNode hnode = {
				.name       = node->name,
				.byteOffset = node->byteOffset,
				.byteSize   = node->byteSize,
				.numPoints  = node->numPoints,
			};

			buffer.push_back(hnode);
            //--------MPI--------------------------
            // We will going to write to hiearchyfile after all the chunks have been processed by a task so that the byTeOffset is correct
			/*if(buffer.size() > 10'000){
				this->write(buffer, hierarchyStepSize);
				buffer.clear();
			}*/
            //---------MPI-----------------------
		}

		void flush(int hierarchyStepSize, int64_t totalOctreefileOffset){
			lock_guard<mutex> lock(mtx);

            cout << "Memory usage of hierarchy buffer: " << (float)(buffer.size() * sizeof(HNode)) / (float)1024 / (float)1024 << " MBytes" << endl;

			this->write(buffer, hierarchyStepSize, totalOctreefileOffset);
			buffer.clear();
		}

		void write(vector<HNode> nodes, int hierarchyStepSize, int64_t totalOctreefileOffset){

			unordered_map<string, vector<HNode>> groups;

			for(auto node : nodes){


				string key = node.name.substr(0, hierarchyStepSize + 1);
				if(node.name.size() <= hierarchyStepSize + 1){
					key = "r";
				}

				if(groups.find(key) == groups.end()){
					groups[key] = vector<HNode>();
				}

				groups[key].push_back(node);

				// add batch roots to batches (in addition to root batch)
				if(node.name.size() == hierarchyStepSize + 1){
					groups[node.name].push_back(node);
				}
			}

			fs::create_directories(path);

			// this structure, but guaranteed to be packed
			// struct Record{                 size   offset
			// 	uint8_t name[31];               31        0
			// 	uint32_t numPoints;              4       31
			// 	int64_t byteOffset;              8       35
			// 	int32_t byteSize;                4       43
			// 	uint8_t end = '\n';              1       47
			// };                              ===
			//                                  48

			
			for(auto [key, groupedNodes] : groups){

				Buffer buffer(48 * groupedNodes.size());
				stringstream ss;

				for(int i = 0; i < groupedNodes.size(); i++){
					auto node = groupedNodes[i];

					auto name = node.name.c_str();
					memset(buffer.data_u8 + 48 * i, ' ', 31);
					memcpy(buffer.data_u8 + 48 * i, name, node.name.size());
					buffer.set<uint32_t>(node.numPoints,  48 * i + 31);
					buffer.set<uint64_t>(node.byteOffset + totalOctreefileOffset, 48 * i + 35);
					buffer.set<uint32_t>(node.byteSize,   48 * i + 43);
					buffer.set<char    >('\n',             48 * i + 47);

					ss << rightPad(name, 10, ' ') 
						<< leftPad(to_string(node.numPoints), 8, ' ')
						<< leftPad(to_string(node.byteOffset + totalOctreefileOffset), 12, ' ')
						<< leftPad(to_string(node.byteSize), 12, ' ')
						<< endl;
				}

				string filepath = path + "/" + key + ".bin";
				fstream fout(filepath, ios::app | ios::out | ios::binary);
				fout.write(buffer.data_char, buffer.size);
				fout.close();

				/*if(chunks.find(key) == chunks.end()){
					chunks[key] = 0;
				}

				chunks[key] += groupedNodes.size();*/
			}

		}

	};

	struct HierarchyChunk {
		string name = "";
		vector<Node*> nodes;
	};

	struct FlushedChunkRoot {
		shared_ptr<Node> node;
		int64_t offset = 0;
		int64_t size = 0;
        int64_t taskID = 0;
	};



    struct FlushedChunkRootSerial {
        char name[12];
        double minx = double(0.0);
        double miny = double(0.0);
        double minz = double(0.0);
        double maxx = double(0.0);
        double maxy = double(0.0);
        double maxz = double(0.0);
        int64_t indexStart = 0;

        int64_t byteOffset = 0;
        int64_t byteSize = 0;
        int64_t numPoints = 0;

        int isSampled = 0;


        int64_t offset = 0;
        int64_t size = 0;
        int64_t colors[];
    };

	struct CRNode{
		string name = "";
		Node* node;
		vector<shared_ptr<CRNode>> children;
		vector<FlushedChunkRoot> fcrs;
		int numPoints = 0;

		CRNode(){
			children.resize(8, nullptr);
		}

		void traverse(function<void(CRNode*)> callback) {
			callback(this);

			for (auto child : children) {

				if (child != nullptr) {
					child->traverse(callback);
				}

			}
		}

		void traversePost(function<void(CRNode*)> callback) {
			for (auto child : children) {

				if (child != nullptr) {
					child->traversePost(callback);
				}
			}

			callback(this);
		}

		bool isLeaf() {
			for (auto child : children) {
				if (child != nullptr) {
					return false;
				}
			}

			return true;
		}
	};

	struct Indexer{

		string targetDir = "";

		Options options;

		Attributes attributes;
		shared_ptr<Node> root;

		shared_ptr<Writer> writer;
		shared_ptr<HierarchyFlusher> hierarchyFlusher;

		vector<shared_ptr<Node>> detachedParts;

		atomic_int64_t byteOffset = 0;

        int64_t octreeFileSize = 0;

        int64_t totalChunks = 0;

        //string indexer_state = "";

        //int64_t totalCompressedBytesinIndexing = 0;
        //int64_t totalCompressedBytesinMerging = 0;
        //int64_t totalBytesWrittenToOctreeFileinIndexing = 0;
        //int64_t totalBytesWrittenToOctreeFileinMerging = 0;


		double scale = 0.001;
		double spacing = 1.0;

		atomic_int64_t dbg = 0;

		mutex mtx_depth;
		int64_t octreeDepth = 0;

		//shared_ptr<TaskPool<FlushTask>> flushPool;
		atomic_int64_t bytesInMemory = 0;
		atomic_int64_t bytesToWrite = 0;
		atomic_int64_t bytesWritten = 0;

        int maxVarSize = 8;

		mutex mtx_chunkRoot;
		fstream fChunkRoots;
        fstream MPISendRcvlog;
		vector<FlushedChunkRoot> flushedChunkRoots;

        FlushedChunkRoot fcrWaiting;


        //vector<uint8_t*> fcrMPIrcv;

        uint8_t *fcrMPIsend = nullptr;
        MPI_Request fcrSendRequest;
        int64_t processOctreeFileOffset = 0;

        int64_t currentTotalOctreeFileSize = 0;

        //vector<MPI_Request> fcrRcvRequest;
        //vector<MPI_Status> fcrRcvStatus;
        //vector<int> fcrRcvFlag;



        //int fcrRcvBufferSize = 1e6;

		Indexer(string targetDir) {

			this->targetDir = targetDir;

			writer = make_shared<Writer>(this);
			hierarchyFlusher = make_shared<HierarchyFlusher>(targetDir + "/hierarchyChunks/hierarchyChunks_" + to_string(process_id));

            //Assuming all other datatypes are smaller than int64 and double.
            maxVarSize = std::max(sizeof(int64_t), sizeof(double));

			string chunkRootFile = targetDir + "/tmpChunkRoots_" + to_string(process_id) + ".bin";
			fChunkRoots.open(chunkRootFile, ios::out | ios::binary);
            string MPIlogFile = targetDir + "/MPISendRcvlog_" + to_string(process_id) + ".txt";
            MPISendRcvlog.open(MPIlogFile, ios::out);

            //fcrMPIrcv.reserve(n_processes - 1);
          //  fcrRcvRequest.reserve(n_processes - 1);
            //fcrRcvStatus.reserve(n_processes - 1);
            //fcrRcvFlag.reserve(n_processes - 1);
		}

		~Indexer() {
			fChunkRoots.close();
            MPISendRcvlog.close();
		}
		void waitUntilWriterBacklogBelow(int maxMegabytes);

		void waitUntilMemoryBelow(int maxMegabytes);

		string createMetadata(Options options, State& state, Hierarchy hierarchy);

		string createDebugHierarchy();

		HierarchyChunk gatherChunk(Node* start, int levels);

		vector<HierarchyChunk> createHierarchyChunks(Node* root, int hierarchyStepSize);


		Hierarchy createHierarchy(string path);

		void flushChunkRoot(shared_ptr<Node> chunkRoot);

		void reloadChunkRoots();

		vector<CRNode> processChunkRoots();

        void sendflushedChunkRoot(FlushedChunkRoot fcr);

        void packAndSend(FlushedChunkRoot fcr);

        void rcvflushedChunkRoot();

        void sendCRdone();


	};

	class punct_facet : public std::numpunct<char> {
	protected:
		char do_decimal_point() const { return '.'; };
		char do_thousands_sep() const { return '\''; };
		string do_grouping() const { return "\3"; }
	};

	shared_ptr<Chunks> doIndexing(string chunksDir, State& state, Options& options, Sampler& sampler, Indexer &indexer, bool islastbatch, shared_ptr<std::map<string, vector<string>>> chunkFiletoPathMap);
    void doFinalMerge(Indexer &indexer, shared_ptr<Chunks> chunks, string targetDir, Sampler &sampler, Options &options,
                      State &state);


}