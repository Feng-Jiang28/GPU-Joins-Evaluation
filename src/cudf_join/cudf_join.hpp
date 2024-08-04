#pragma once
#include <cassert>
#include <iostream>

using namespace std;

enum join_type {
    PK_FK,
    FK_FK,
    NOT_SUPPORTED_JOIN_TYPE
};

enum dist_type {
    UNIFORM,
    ZIPF
};


enum join_algo {
    CUDF_JOIN,
    PHJ,
    SMJ,
    SHJ,
    SMJI,
    UnsupportedJoinAlgo
};

struct join_args {
    int nr {4096};
    int ns {4096};
    int pr {1};
    int ps {1};
    int vec_size {8192};
    int unique_keys {4096};
    enum join_type type {PK_FK};
    enum dist_type dist {UNIFORM};
    double zipf_factor {1.5};
    int selectivity {1};
    bool agg_only {false};
    std::string output {"join_exp.csv"};
    bool late_materialization {false};
    enum join_algo algo {SMJ};
    int phj_log_part1 {9};
    int phj_log_part2 {6};
    std::string data_path_prefix {"/scratch/wubo/joinmb/"};
#ifndef KEY_T_8B
    int key_bytes {4};
#else
    int key_bytes {8};
#endif
#ifndef COL_T_8B
    int val_bytes {4};
#else
    int val_bytes {8};
#endif

    void print() {
        cout << "||R|| = " << nr << " "
             << "||S|| = " << ns << "\n"
             << "R payload columns = " << pr << " "
             << "S payload columns = " << ps << "\n"
             << "Join algorithm: CUDF JOIN \n"
             << "Join type: " << (type == PK_FK ? "Primary-foreign" : "Unique") << "\n"
             << "(if PK-FK) Distribution type: " << (dist == UNIFORM ? "Uniform" : "Zipf") << "\n"
             << "(if zipf) factor = " << zipf_factor << "\n"
             << "(if PK-FK) Selectivity = " << selectivity << "\n"
             << "(if PHJ) log_part1 = " << phj_log_part1 << " log_part2 = " << phj_log_part2 << "\n"
             << "key_bytes = " << key_bytes << " val_bytes = " << val_bytes << "\n"
             << "Late Materialization only? " << (late_materialization ? "Yes" : "No") << "\n"
             << "Output file: " << output << "\n"
             << "Data path prefix: " << data_path_prefix << "\n"
             << "Aggregation only? " << (agg_only ? "Yes" : "No") << "\n\n";
    }

    void check() {
        assert(pr >= 0 && ps >= 0);
        assert(!output.empty());
        assert(algo < UnsupportedJoinAlgo);
        if(type == FK_FK) assert(unique_keys <= nr && unique_keys <= ns);
    }

};

void prepare_running(int argc, char** argv);

template<typename TupleR, typename TupleS, typename TupleOut>
void free_tuple_mem(TupleR r, TupleS s, TupleOut out);

template<typename join_key_t, typename col_t, typename TupleR, typename TupleS>
void prepare_workload(const struct join_args& args, TupleR& relation_r, TupleS& relation_s);

inline std::string get_utc_time();

void say_hello();