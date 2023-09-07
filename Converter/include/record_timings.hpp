//
// Created by nahmed on 17-08-20.
//

#pragma once

#include <chrono>
#include <stack>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <map>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <iterator>
#include <linux/param.h>
#ifdef __CUDACC__
#include <cuda_runtime.h>
#endif
#include<tuple>

#include <unistd.h>
#include <sys/syscall.h>
#include <pthread.h>




namespace recordTimings{

using namespace std;

    enum class Machine {
        cpu,
        gpu

    };

    typedef struct {
        string desc;
        float time_diff;
        uint64_t flops;
        Machine t;
        string parent_desc;
        vector<tuple<string, float>> parent_times;
        int level;
    } time_record_to_save;



    class Record_timings {
    public:
        Record_timings()  {}


        void start_timing(Machine t, const string &desc, const int line_start, const string &file_name) {

            struct time_record_all rec;
            rec.recordToSave.desc = desc;
            rec.recordToSave.t = t;
            rec.line_start = line_start;
#ifdef __CUDACC__
            if (t != Machine::cpu) {
                cudaError_t error;
                for (auto & event : rec.cuda_events) {
                    error = cudaEventCreate(&event);
                    if (error != cudaSuccess) {
                        cerr << "cudaEventCreate() failed: " << cudaGetErrorString(error) << " on line number " <<  line_start << " in file " <<
                                  file_name <<  endl;
                        exit(EXIT_FAILURE);
                    }
                }
                error = cudaEventRecord(rec.cuda_events[0]);
                if (error != cudaSuccess) {
                    cerr << "cudaEventRecord() failed: " << cudaGetErrorString(error) << " on line number " <<  line_start << " in file " <<
                              file_name <<  endl;
                    exit(EXIT_FAILURE);
                }
            }
            else {
#endif
            rec.timePoints[0] = chrono::high_resolution_clock::now();
#ifdef __CUDACC__
            }
#endif
            rec.recordToSave.level = starts.size();


            starts.push(rec);

        }

        void stop_timing(Machine t, const string &desc, const int line_stop, const string &file_name,
                         uint64_t flops = 0) {

            struct time_record_all rec = starts.top();


            if (t != rec.recordToSave.t || desc != rec.recordToSave.desc) {
                cerr << "RECORD_TIMING_START on line no. " << rec.line_start
                          << " do not match with RECORD_TIMING_STOP on line no. " << line_stop << " in file "
                          << file_name << endl;
                exit(EXIT_FAILURE);

            }
#ifdef __CUDACC__
            if (t != Machine::cpu){
                cudaError_t error;
                error = cudaEventRecord(rec.cuda_events[1]);
                if (error != cudaSuccess) {
                    cerr << "cudaEventRecord() failed: " << cudaGetErrorString(error) << " on line number " <<  line_stop << " in file " <<
                              file_name <<  endl;
                    exit(EXIT_FAILURE);
                }
                error = cudaEventSynchronize(rec.cuda_events[1]);
                if (error != cudaSuccess) {
                    cerr << "cudaEventSynchronize() failed: " << cudaGetErrorString(error) << " on line number " <<  line_stop << " in file "
                              << file_name <<  endl;
                    exit(EXIT_FAILURE);
                }

            }
            else {
#endif
            rec.timePoints[1] = chrono::high_resolution_clock::now();
#ifdef __CUDACC__
            }
#endif

            starts.pop();

            if (starts.size() > 0) {
                rec.recordToSave.parent_desc = starts.top().recordToSave.desc;

            } else rec.recordToSave.parent_desc = "";

            /*struct find_timing_record : unary_function<time_record_to_save, bool> {
                time_record_to_save record;

                find_timing_record(time_record_to_save id) : record(id) {}

                bool operator()(time_record_to_save const &m) const {
                    return (m.t == record.t && m.desc == record.desc);
                }
            };*/

            float diff = 0;
#ifdef __CUDACC__
            if (t != Machine::cpu) {
                cudaError_t error = cudaEventElapsedTime(&diff, rec.cuda_events[0], rec.cuda_events[1]);
                if (error != cudaSuccess) {
                    cerr << "cudaEventElapsedTime() failed: " << cudaGetErrorString(error) << " on line number " <<  line_stop << " in file " <<
                              file_name <<  endl;
                    exit(EXIT_FAILURE);
                }
                for (auto event : rec.cuda_events) {
                    error = cudaEventDestroy(event);
                    if (error != cudaSuccess) {
                        cerr << "cudaEventDestroy() failed: " << cudaGetErrorString(error) << " on line number "
                                  << line_stop << " in file " << file_name << endl;
                        exit(EXIT_FAILURE);
                    }
                }
            }
            else {
#endif
            diff = chrono::duration<float, ratio<1, 1000>>(rec.timePoints[1] - rec.timePoints[0]).count();
#ifdef __CUDACC__
            }
#endif

            //auto it = find_if(timeStamps.begin(), timeStamps.end(), find_timing_record(rec.recordToSave));



            if (timeStamps.contains(desc)) {
                time_record_to_save *sec = &timeStamps.at(desc);
                sec->time_diff += diff;
                sec->flops += flops;


            } else {
                rec.recordToSave.time_diff = diff;
                rec.recordToSave.flops = flops;
                timeStamps.emplace(desc, rec.recordToSave);
            }

            //starts.pop();




        }


        /*void
        start_timing_wrapper(Machine t, const string &desc, const int line_start, const string &file_name) {
            start_timing(t, desc, line_start, file_name);
        }

        void stop_timing_wrapper(Machine t, const string &desc, const int line_stop, const string &file_name,
                                 uint64_t flops) {
            stop_timing(t, desc, line_stop, file_name, flops);
        }*/

        size_t size() {
            return timeStamps.size();
        }

        map<string, time_record_to_save> &timeStamps_map() {
            return timeStamps;
        }

    private:

        struct time_record_all {
            time_record_to_save recordToSave;
            int line_start;
            chrono::high_resolution_clock::time_point timePoints[2];
#ifdef __CUDACC__
            cudaEvent_t cuda_events[2];
#endif
        };
        map<string, time_record_to_save> timeStamps;
        stack<time_record_all> starts;

    };

    extern map<pid_t, Record_timings> thread_time_record_map;


    extern map<string, vector<pid_t>> desc_thread_map;

    extern pthread_mutex_t thread_time_record_map_mtx;

    extern pthread_mutex_t desc_thread_map_mtx;


    static inline pid_t get_thread_id() {

#ifdef SYS_gettid
        return syscall(SYS_gettid);
#else
#error "SYS_gettid unavailable on this system"
#endif
    }




    static inline void thread_start_timing(Machine t, const string &desc, const int line_start, const string &file_name) {



        pid_t thread_id = get_thread_id();
        pthread_mutex_lock(&desc_thread_map_mtx);
        if (desc_thread_map.contains(desc)){
            if (find(desc_thread_map.at(desc).cbegin(), desc_thread_map.at(desc).cend(), thread_id) == desc_thread_map.at(desc).cend()) {
                desc_thread_map.at(desc).push_back(thread_id);
            }

        } else{
            //cout << "1Descriptor, Thread ID" << desc << "," << thread_id << "\n";
            vector<pid_t> tmp;
            tmp.push_back(thread_id);
            desc_thread_map.emplace(desc, tmp);
        }
        pthread_mutex_unlock(&desc_thread_map_mtx);

        pthread_mutex_lock(&thread_time_record_map_mtx);
        if(thread_time_record_map.contains(thread_id)){
            pthread_mutex_unlock(&thread_time_record_map_mtx);
            thread_time_record_map.at(thread_id).start_timing(t, desc, line_start, file_name);
        }
        else{
            thread_time_record_map.emplace(thread_id, Record_timings());
            pthread_mutex_unlock(&thread_time_record_map_mtx);
            thread_time_record_map.at(thread_id).start_timing(t, desc, line_start, file_name);
        }



    }


    static inline void thread_stop_timing(Machine t, const string &desc, const int line_stop, const string &file_name,
                       uint64_t flops = 0) {

        pid_t thread_id = get_thread_id();

        pthread_mutex_lock(&thread_time_record_map_mtx);
        if(thread_time_record_map.contains(thread_id)){
            pthread_mutex_unlock(&thread_time_record_map_mtx);
            thread_time_record_map.at(thread_id).stop_timing(t, desc, line_stop, file_name);
        }else {
            cerr << "Unable to able to find thread " << thread_id << " record while stop timimg" << endl;
            pthread_mutex_unlock(&thread_time_record_map_mtx);
            exit(EXIT_FAILURE);
        }

    }

    /*void
    find_parent(const int i, const string &parent_desc, vector<tuple<string, float>> &parent_times,
                vector<time_record_to_save> &timeStamps) {
        if (i >= timeStamps.size()) return;
        if (timeStamps[i].desc == parent_desc) {
            parent_times.push_back(make_tuple(parent_desc, timeStamps[i].time_diff));
            find_parent(i + 1, timeStamps[i].parent_desc, parent_times, timeStamps);
        } else {
            find_parent(i + 1, parent_desc, parent_times, timeStamps);
        }

    }

    void print_timings(ostream &stream, vector<time_record_to_save> &timeStamps) {

        for (int i = 0; i < timeStamps.size(); i++) {
            if (timeStamps[i].level != 0)
                find_parent(i + 1, timeStamps[i].parent_desc, timeStamps[i].parent_times, timeStamps);
            //stream << timeStamps[i].desc << ":" << timeStamps[i].parent_desc << endl;

        }
        stream << "=========================Record_timings results=================================" << endl;

        for (auto idx = timeStamps.cend() - 1; idx >= timeStamps.cbegin(); idx--) {
            for (int i = 0; i < idx->parent_times.size(); i++) {
                stream << "\t";
            }
            stream << "Time spent in \"" << idx->desc << "\" on " << (idx->t == Machine::cpu ? "CPU" : "GPU") << ": "
                   << idx->time_diff << "ms";
            if (!idx->parent_times.empty()) {
                stream << " (";
                for (int i = 0; i < idx->parent_times.size(); i++) {
                    if (get<1>(idx->parent_times[i]) > 0) {
                        stream << (idx->time_diff / get<1>(idx->parent_times[i])) * 100 << "% of "
                               << get<0>(idx->parent_times[i]);
                        if (i > idx->parent_times.size() - 1) stream << ", ";
                    }
                }
                stream << ")";
            }
            if (idx->flops > 0) {
                stream << " GFLOPS: " << ((double) idx->flops / (double) 1e9) / (idx->time_diff * 1e-3);
            }
            stream << endl;
        }

        stream << "================================================================================" << endl;


    }*/


    static inline float get_process_time(){

            int fd;
            char buff[128];
            char *p;
            float uptime = 0.0, process_time;
            //struct timeval tv;
            //static time_t process_time;


            if ((fd = open("/proc/uptime", 0)) != -1)
            {
                if (read(fd, buff, sizeof(buff)) > 0)
                {
                    uptime = strtof(buff, &p);
                    //gettimeofday(&tv, 0);
                    //now = tv.tv_sec;
                    //boottime = tv.tv_sec - uptime;

                }
                else {
                    cerr << "Unable to read /proc/uptime\n";
                    exit(EXIT_FAILURE);
                }
                close(fd);
            } else {
                cerr << "Unable to open /proc/uptime\n";
                exit(EXIT_FAILURE);
            }



            ifstream procFile;

            char process_path[255];

            ::sprintf(process_path, "/proc/%i/stat", getpid());
            procFile.open(process_path);

            char str[255];
            procFile.getline(str, 255);  // delim defaults to '\n'


            vector<string> tmp;
            istringstream iss(str);
            copy(istream_iterator<string>(iss),
                 istream_iterator<string>(),
                 back_inserter<vector<string> >(tmp));

            //process_time = (now - boottime) - (atof(tmp.at(21).c_str()))/HZ;

            process_time = uptime - (atof(tmp.at(21).c_str()))/HZ;

            return process_time;


    }

    static inline void thread_print_timing(ostream &stream) {


        float total_time = get_process_time();

        stream << "\n=========================Record_timings results=================================" << endl;

        stream << "Total runtime=" << total_time << " seconds\n";




        for(auto& it_desc_map: desc_thread_map){
            float avg_time = 0.0;
            Machine m;
            for (auto& it_record_map : it_desc_map.second) {
                Record_timings &a = thread_time_record_map.at(it_record_map);
                avg_time += a.timeStamps_map().at(it_desc_map.first).time_diff/it_desc_map.second.size();
                m =  a.timeStamps_map().at(it_desc_map.first).t;
            }
            stream << "\tAverage Time spent in \"" << it_desc_map.first << "\" by " << it_desc_map.second.size()   << " threads on "  << (m == Machine::cpu ? "CPU" : "GPU") << "="
                   << avg_time << " ms" << ", which is " <<  (avg_time/(total_time * 1e3))*100 << "% of the total runtime\n";
        }
        stream << "================================================================================" << endl;


    }

    extern bool disable;

}


#define RECORD_TIMINGS_DISABLE() {recordTimings::disable = true;}

#define RECORD_TIMINGS_INIT()\
    map<pid_t, recordTimings::Record_timings> recordTimings::thread_time_record_map;\
    map<string, vector<pid_t>> recordTimings::desc_thread_map;\
    pthread_mutex_t recordTimings::thread_time_record_map_mtx = PTHREAD_MUTEX_INITIALIZER;\
    pthread_mutex_t recordTimings::desc_thread_map_mtx = PTHREAD_MUTEX_INITIALIZER;\
    bool recordTimings::disable = false;


#define RECORD_TIMINGS_START(machine, desc_str) \
    if (!recordTimings::disable)                 \
    {                                           \
        recordTimings::thread_start_timing((machine), (desc_str),  __LINE__, __FILE__); \
    }

#define RECORD_TIMINGS_STOP(machine, desc_str) \
    if (!recordTimings::disable)               \
    {                                          \
        recordTimings::thread_stop_timing((machine), (desc_str),  __LINE__, __FILE__); \
    }

#define RECORD_TIMINGS_STOP_WITH_FLOPS(machine, desc_str, flops)  \
    if (!recordTimings::disable)                                   \
    {                                                              \
        recordTimings::thread_stop_timing((machine), (desc_str), __LINE__, __FILE__, flops); \
    }

#define RECORD_TIMINGS_PRINT(stream) \
    if (!recordTimings::disable)     \
    {                                \
        recordTimings::thread_print_timing(stream); \
    }
