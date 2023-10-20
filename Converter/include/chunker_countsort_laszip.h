
#pragma once

#include <string>
#include <vector>

#include "Vector3.h"
#include "Attributes.h"
#include "Monitor.h"

using std::string;
using std::vector;

class Source;
class State;


namespace chunker_countsort_laszip {

    // grid contains index of node in nodes
    struct NodeLUT {
        int64_t gridSize;
        vector<int> grid;
    };

	NodeLUT doCounting(Vector3 min, Vector3 max, State& state, string targetDir, Attributes &outputAttributes, Monitor* monitor);
    void doDistribution(Vector3 min, Vector3 max, State& state, NodeLUT lut, string chunksDir, vector<Source> sources, Attributes &outputAttributes, Monitor* monitor);





}