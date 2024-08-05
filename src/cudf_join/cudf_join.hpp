#pragma once
#include <cassert>
#include <iostream>
#include "tuple_wrapper.hpp"
#include <fstream>
#include <cudf/join.hpp>

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

#define RUN_CASE(c1, c2, c3) { \
    if(args.pr+1 == c1 && args.ps+1 == c2) { \
        if(args.type == PK_FK || args.type == FK_FK) { \
            run_test_multicols<join_key_t, col_t, TU ## c1, TU ## c2, TU ## c3>(args); \
        } \
    } \
}

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

//void prepare_running(int argc, char** argv);

template<typename TupleR, typename TupleS, typename TupleOut>
void free_tuple_mem(TupleR r, TupleS s, TupleOut out);


template<typename join_key_t, typename col_t, typename TupleR, typename TupleS>
void prepare_workload(const struct join_args& args, TupleR& relation_r, TupleS& relation_s);

inline std::string get_utc_time();


template<typename JoinImpl>
void exp_stats(JoinImpl* impl, const struct join_args& args) {
    cout << "\n==== Statistics ==== \n";
    impl->print_stats();
    //cout << "Peak memory used: " << mm->get_peak_mem_used() << " bytes\n";

    if(!args.output.empty()) {
        ofstream fout;
        fout.open(args.output, ios::app);
        fout << get_utc_time() << ","
             << args.nr << "," << args.ns << ","
             << args.pr << "," << args.ps << ", cudf join"
             << (args.type == PK_FK ? "pk_fk," : "fk_fk,")
             << args.unique_keys << ","
             << (args.dist == UNIFORM ? "uniform," : "zipf,")
             << args.zipf_factor << ","
             << args.selectivity << ","
             << (args.agg_only ? "aggregation," : "materialization,")
             << args.phj_log_part1 << "," << args.phj_log_part2 << ","
             << args.key_bytes << "," << args.val_bytes << ",";

        auto stats = impl->all_stats();
        for(auto t : stats) {
            fout << t << ",";
        }

        fout << endl;
        fout.close();
    }
}

//template<typename TupleR, typename TupleS, typename ResultOut>
//ResultOut exec_join(TupleR r, TupleS s);

template<typename join_key_t, typename col_t, typename TupleR, typename TupleS, typename ResultTuple>
void run_test_multicols(const struct join_args& args) {
    TupleR relation_r;
    TupleS relation_s;
    prepare_workload<join_key_t, col_t>(args, relation_r, relation_s);
    ResultTuple out;

    /*
    cout << "\nOutput Cardinality = " << out.num_items << endl;
    cout << "Results (first 10 items): \n";
    out.peek(args.agg_only ? 1 : min(10, out.num_items));
    */

    free_tuple_mem(relation_r, relation_s, out);
}

int main(int argc, char** argv){
    //cout << "hello world! \n";
#ifndef COL_T_8B
    using col_t = int;
#else
    using col_t = long;
#endif

#ifndef KEY_T_8B
    using join_key_t = int;
#else
    using join_key_t = long;
#endif
    DECL_TUP_1_TO_8(join_key_t, col_t);
    //prepare_running(argc, argv);
    struct join_args args;
    // increasing number of payloads in FK table
    RUN_CASE(2, 3, 4);
    RUN_CASE(2, 4, 5);
    RUN_CASE(2, 5, 6);
    RUN_CASE(2, 6, 7);
    RUN_CASE(2, 7, 8);

    // increasing number of payloads in FK table
    RUN_CASE(3, 2, 4);
    RUN_CASE(4, 2, 5);
    RUN_CASE(5, 2, 6);
    RUN_CASE(6, 2, 7);
    RUN_CASE(7, 2, 8);

    // both sides have payload columns to materialize
    RUN_CASE(2, 2, 3);
    RUN_CASE(3, 3, 5);
    RUN_CASE(4, 4, 7);
    RUN_CASE(5, 5, 9);
    RUN_CASE(6, 6, 11);
    RUN_CASE(7, 7, 13);

    return 0;
}