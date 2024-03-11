
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

    // The function that counts points in the grid one partition/batch at a time
	NodeLUT doCounting(Vector3 min, Vector3 max, State& state, string targetDir, Attributes &outputAttributes, Monitor* monitor);
    // THe function for distributing points into chunks
    void doDistribution(Vector3 min, Vector3 max, State& state, NodeLUT lut, string chunksDir, vector<Source> sources, Attributes &outputAttributes, Monitor* monitor);





}