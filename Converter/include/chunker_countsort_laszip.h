
#pragma once

#include <string>
#include <vector>

#include "Vector3.h"
#include "Attributes.h"
#include "Monitor.h"

using std::string;
using std::vector;

class DataFile;
class State;

namespace chunker_countsort_laszip {

	void doChunking(Vector3 min, Vector3 max, State& state, Attributes outputAttributes, Monitor* monitor);

    // grid contains index of node in nodes
    struct NodeLUT {
        int64_t gridSize;
        vector<int> grid;
    };



}