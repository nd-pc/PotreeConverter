
#pragma once

#include <memory>
#include <string>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <thread>
#include <mutex>
#include <atomic>
#include <map>
#include <execution>

#include "LasLoader/LasLoader.h"
#include "unsuck/unsuck.hpp"
#include "Vector3.h"

using std::ios;
using std::thread;
using std::mutex;
using std::lock_guard;
using std::atomic_int64_t;

namespace fs = std::filesystem;

struct LASPointF2 {
	int32_t x;
	int32_t y;
	int32_t z;
	uint16_t intensity;
	uint8_t returnNumber;
	uint8_t classification;
	uint8_t scanAngleRank;
	uint8_t userData;
	uint16_t pointSourceID;
	uint16_t r;
	uint16_t g;
	uint16_t b;
};

enum SourceFileType{
    DATA,
    HEADER
};

struct vlr {
    vector<uint8_t> data;
    uint16_t recordID;

    std::string toString() const {
        std::stringstream ss;
        ss << "Record ID: " << recordID << ", Data: [";
        for (size_t i = 0; i < data.size(); ++i) {
            ss << static_cast<int>(data[i]);
        }
        ss << "]";
        return ss.str();
    }
};

class Source {
public:
    std::string path;
    uint64_t filesize;
    uint64_t numPoints;
    uint16_t pointDataRecordLength;
    int bytesPerPoint;
    Vector3 min;
    Vector3 max;
    Vector3 scale;
    uint8_t pointDataFormat;
    std::vector<vlr> vlrs;
    SourceFileType type;

    // Overloaded toString function
    std::string toString() const {
        std::stringstream ss;
        ss << "Path: " << path << "\n";
        ss << "Filesize: " << filesize << " bytes\n";
        ss << "Number of Points: " << numPoints << "\n";
        ss << "Point Data Record Length: " << pointDataRecordLength << "\n";
        ss << "Bytes Per Point: " << bytesPerPoint << "\n";
        ss << "Min: " << "[" << min.toString() << "]" << "\n";
        ss << "Max: " << "[" << max.toString() << "]" "\n";
        ss << "Scale: " << "[" << scale.toString() << "]" "\n";
        ss << "Point Data Format: " << static_cast<int>(pointDataFormat) << "\n";
        ss << "Variable Length Records (VLRs):\n";
        for (const auto& vlr : vlrs) {
            ss << vlr.toString() << "\n";
        }
        // Add other members as needed

        return ss.str();
    }
};

inline vector<Source> curateSources(vector<string> paths) {



    vector<Source> sources;
    sources.reserve(paths.size());

    mutex mtx;
    auto parallel = std::execution::par;
    for_each(parallel, paths.begin(), paths.end(), [&mtx, &sources](string path) {

        auto header = loadLasHeader(path);
        //auto filesize = fs::file_size(path);

        Vector3 min = { header.min.x, header.min.y, header.min.z };
        Vector3 max = { header.max.x, header.max.y, header.max.z };
        Vector3 scale = { header.scale.x, header.scale.y, header.scale.z };

        Source source;
        source.path = path;
        source.min = min;
        source.max = max;
        source.scale = scale;
        source.pointDataRecordLength = header.pointDataRecordLength;
        source.numPoints = header.numPoints;
        source.filesize = header.numPoints * header.pointDataRecordLength;
        source.pointDataFormat = header.pointDataFormat;
        vector<vlr> vlrs;
        int n = 0;
        for(auto r : header.vlrs){
            vlr v;
            v.data = r.data;
            v.recordID = r.recordID;
        }


        source.type = SourceFileType::DATA;

        lock_guard<mutex> lock(mtx);
        sources.push_back(source);
    });

    return sources;
}
struct State {

    string name = "";
	atomic_int64_t pointsTotal = 0;
	atomic_int64_t pointsProcessed = 0;
	atomic_int64_t bytesProcessed = 0;
	double duration = 0.0;
	std::map<string, string> values;

	int numPasses = 3;
	int currentPass = 0; // starts with index 1! interval: [1,  numPasses]

	mutex mtx;

	double progress() {
		return double(pointsProcessed) / double(pointsTotal);
	}
};


struct BoundingBox {
	Vector3 min;
	Vector3 max;

	BoundingBox() {
		this->min = { Infinity,Infinity,Infinity };
		this->max = { -Infinity,-Infinity,-Infinity };
	}

	BoundingBox(Vector3 min, Vector3 max) {
		this->min = min;
		this->max = max;
	}
};


// see https://www.forceflow.be/2013/10/07/morton-encodingdecoding-through-bit-interleaving-implementations/
// method to seperate bits from a given integer 3 positions apart
inline uint64_t splitBy3(unsigned int a) {
	uint64_t x = a & 0x1fffff; // we only look at the first 21 bits
	x = (x | x << 32) & 0x1f00000000ffff; // shift left 32 bits, OR with self, and 00011111000000000000000000000000000000001111111111111111
	x = (x | x << 16) & 0x1f0000ff0000ff; // shift left 32 bits, OR with self, and 00011111000000000000000011111111000000000000000011111111
	x = (x | x << 8) & 0x100f00f00f00f00f; // shift left 32 bits, OR with self, and 0001000000001111000000001111000000001111000000001111000000000000
	x = (x | x << 4) & 0x10c30c30c30c30c3; // shift left 32 bits, OR with self, and 0001000011000011000011000011000011000011000011000011000100000000
	x = (x | x << 2) & 0x1249249249249249;
	return x;
}

// see https://www.forceflow.be/2013/10/07/morton-encodingdecoding-through-bit-interleaving-implementations/
inline uint64_t mortonEncode_magicbits(unsigned int x, unsigned int y, unsigned int z) {
	uint64_t answer = 0;
	answer |= splitBy3(x) | splitBy3(y) << 1 | splitBy3(z) << 2;
	return answer;
}

inline BoundingBox childBoundingBoxOf(Vector3 min, Vector3 max, int index) {
	BoundingBox box;
	auto size = max - min;
	Vector3 center = min + (size * 0.5);

	if ((index & 0b100) == 0) {
		box.min.x = min.x;
		box.max.x = center.x;
	} else {
		box.min.x = center.x;
		box.max.x = max.x;
	}

	if ((index & 0b010) == 0) {
		box.min.y = min.y;
		box.max.y = center.y;
	} else {
		box.min.y = center.y;
		box.max.y = max.y;
	}

	if ((index & 0b001) == 0) {
		box.min.z = min.z;
		box.max.z = center.z;
	} else {
		box.min.z = center.z;
		box.max.z = max.z;
	}

	return box;
}

inline void dbgPrint_ts_later(string message, bool now = false) {
	
	static vector<string> data;

	static mutex mtx;
	lock_guard<mutex> lock(mtx);

	data.push_back(message);

	if (now) {
		for (auto& message : data) {
			cout << message << endl;
		}

		data.clear();
	} 

	
}


struct Options {
	string encoding = "DEFAULT"; // "BROTLI", "UNCOMPRESSED"
	string outdir = "";
    string headerDir = "";
	string name = "";
	string method = "";
	string chunkMethod = "";
	//vector<string> flags;
	vector<string> attributes;
	bool generatePage = false;
	string pageName = "";
	string pageTitle = "";
	string projection = "";

	bool keepChunks = false;
	bool noChunking = false;
	bool noIndexing = false;
    string manualBounds = "";
    int memoryBudget = getMemoryData().physical_total/(1024 * 1024 * 1024);

};