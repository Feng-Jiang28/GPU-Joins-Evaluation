#include <iostream>
#include <vector>
#include <tuple>
#include <chrono>
#include <unistd.h>
#include <fstream>

#include "../volcano/operators.cuh"
#include "experiment_util.cuh"


#include "cudf_join.hpp"
using namespace std;

enum Input {
    RelR,
    RelS,
    UniqueKeys
};

std::string get_path_name(enum Input table, const struct join_args& args) {
    auto nr = args.nr;
    auto ns = args.ns;
    auto uk = args.unique_keys;

#ifndef KEY_T_8B
    std::string subfolder = "int/";
#else
    std::string subfolder = "long/";
#endif

    if(table == UniqueKeys) {
        return args.data_path_prefix+subfolder+"r_" + std::to_string(uk) + ".bin";
    }

    if(args.type == PK_FK) {
        return table == RelR ? args.data_path_prefix+subfolder+"r_" + std::to_string(nr) + ".bin"
                             : args.data_path_prefix+subfolder+"s_" + std::to_string(nr) + "_" +std::to_string(ns) + "_" + (args.dist == UNIFORM ? "uniform" : "zipf_") + (args.dist == UNIFORM ? "" : std::to_string(args.zipf_factor))+".bin";
    }
    else {
        return table == RelR ? args.data_path_prefix+subfolder+"s_" + std::to_string(args.unique_keys) + "_" +std::to_string(nr) + "_uniform.bin"
                             : args.data_path_prefix+subfolder+"s_" + std::to_string(args.unique_keys) + "_" +std::to_string(ns) + "_uniform.bin";
    }
}

template<typename join_key_t, typename col_t, typename TupleR, typename TupleS>
void prepare_workload(const struct join_args& args, TupleR& relation_r, TupleS& relation_s) {
    constexpr int R_NUM_COLS = TupleR::num_cols, S_NUM_COLS = TupleS::num_cols;

    auto nr = args.nr;
    auto ns = args.ns;

    join_key_t *rkeys = nullptr, *skeys = nullptr;
    std::array<col_t*, R_NUM_COLS-1> r;
    std::array<col_t*, S_NUM_COLS-1> s;

    std::string rpath = get_path_name(RelR, args);
    std::string spath = get_path_name(RelS, args);

    if(args.type == PK_FK) {
        // create relation R
        if(input_exists(rpath)) {
            cout << "R read from disk\n";
            alloc_load_column(rpath, rkeys, nr);
        } else {
            create_integral_relation_unique(&rkeys, nr, false, static_cast<join_key_t>(0), true, 42);
            write_to_disk(rkeys, nr, rpath);
        }

        // create relation S
        if(input_exists(spath)) {
            cout << "S read from disk\n";
            alloc_load_column(spath, skeys, ns);
        } else {
            if(args.dist == UNIFORM) {
                create_fk_from_pk_uniform(&skeys, ns, rkeys, nr);
            }
            else {
                create_fk_from_pk_zipf(&skeys, ns, rkeys, nr, args.zipf_factor);
            }

            write_to_disk(skeys, ns, spath);
        }
    }
    else if(args.type == FK_FK) {
        if(args.dist == ZIPF) {
            std::cout << "FKFK join with zipf distribution is not supported for now\n";
            std::exit(-1);
        }

        join_key_t* uk = nullptr;
        auto nuk = args.unique_keys;
        if(!input_exists(rpath) || !input_exists(spath)) {
            std::string upath = get_path_name(UniqueKeys, args);
            if(input_exists(upath)) {
                cout << "Unique keys read from disk\n";
                alloc_load_column(upath, uk, nuk);
            } else {
                create_integral_relation_unique(&uk, nuk, false, static_cast<join_key_t>(0), true, 42);
                write_to_disk(uk, nuk, upath);
            }
        }

        if(input_exists(rpath)) {
            cout << "R read from disk\n";
            alloc_load_column(rpath, rkeys, nr);
        } else {
            create_fk_from_pk_uniform(&rkeys, nr, uk, nuk);
            write_to_disk(rkeys, nr, rpath);
        }

        // create relation S
        if(input_exists(spath)) {
            cout << "S read from disk\n";
            alloc_load_column(spath, skeys, ns);
        } else {
            create_fk_from_pk_uniform(&skeys, ns, uk, nuk);
            write_to_disk(skeys, ns, spath);
        }
    }
    else {
        std::cout << "Unsupported join type\n";
        std::exit(-1);
    }

#ifdef MR_FILTER_FK
    if(args.selectivity > 1) {
        if(args.selectivity >= args.nr) assert(false);
        #pragma omp parallel for
        for(int i = 0; i < nr; i++) {
            if(i % args.selectivity == 0) continue;
            rkeys[i] += (1<<30);
        }
    }
    std::cout << "Filtered FK to reduce the match ratio\n";
#endif

#ifdef SORTED_REL
    sort_on_gpu(rkeys, nr);
    sort_on_gpu(skeys, ns);
#endif

    if(sizeof(col_t) == sizeof(join_key_t)) {
        for(int i = 0; i < S_NUM_COLS-1; i++) {
            s[i] = new col_t[ns];
            memcpy(s[i], skeys, sizeof(col_t)*ns);
        }

        for(int i = 0; i < R_NUM_COLS-1; i++) {
            r[i] = new col_t[nr];
            memcpy(r[i], rkeys, sizeof(col_t)*nr);
        }
    } else {
        for(int i = 0; i < S_NUM_COLS-1; i++) {
            s[i] = new col_t[ns];
        }

        for(int i = 0; i < R_NUM_COLS-1; i++) {
            r[i] = new col_t[nr];
        }

        #pragma unroll
        for(int i = 0; i < ns; i++) {
            s[0][i] = static_cast<col_t>(skeys[i]);
        }

        #pragma unroll
        for(int i = 0; i < nr; i++) {
            r[0][i] = static_cast<col_t>(rkeys[i]);
        }

        for(int i = 1; i < S_NUM_COLS-1; i++) {
            memcpy(s[i], s[0], sizeof(col_t)*ns);
        }

        for(int i = 1; i < R_NUM_COLS-1; i++) {
            memcpy(r[i], r[0], sizeof(col_t)*nr);
        }
    }

    cout << "Data preparation is done\n";

    auto b_cols = std::tuple_cat(std::make_tuple(rkeys), std::tuple_cat(r));
    auto p_cols = std::tuple_cat(std::make_tuple(skeys), std::tuple_cat(s));

    ScanOperator<TupleR> op1(std::move(b_cols), nr, nr);
    ScanOperator<TupleS> op2(std::move(p_cols), ns, ns);

    op1.open(); op2.open();
    relation_r = op1.next();
    relation_s = op2.next();
    op1.close(); op2.close();

    // adjust the match ratio
    // if the match ratio is 1 out of M,
    // then we randomly remove floor(|R|/M) elements from relation R (assuming M < |R|)
    // this is simulating the filtering before join
#ifndef MR_FILTER_FK
    if(args.selectivity > 1) {
        if(args.selectivity >= args.nr) assert(false);
        relation_r.num_items /= args.selectivity;
    }
    cout << "The effective |R| after adjusting the selectivity is " << relation_r.num_items << endl;
#endif

    release_mem(relation_r.select_vec);
    release_mem(relation_s.select_vec);
    relation_r.select_vec = nullptr;
    relation_s.select_vec = nullptr;

    delete[] rkeys;
    delete[] skeys;
    for(int i = 0; i < R_NUM_COLS-1; i++) {
        delete [] r[i];
    }

    for(int i = 0; i < S_NUM_COLS-1; i++) {
        delete [] s[i];
    }
}


void print_usage() {
    cout << "Join Microbenchmarks\n";
    cout << "Usage: <binary> [-l|-h] -r <log_2(|R|)> -s <log_2(|S|)> -m <R payload cols> -n <S payload cols> -t <join type> -d <distribution> -z <zipf factor> -o <output file> -f <data path prefix> -e <selectivity> -u <unique keys> -i <join algorithm> -p <phj log part1> -q <phj log part2>\n";
    cout << "Options:\n";
    cout << "-l: use log scale for |R|, |S|, and unique keys. Default: no.\n";
    cout << "-h: print this message\n";
    cout << "-r: log_2(|R|) if using -l flag otherwise the actual size\n";
    cout << "-s: log_2(|S|) if using -l flag otherwise the actual size\n";
    cout << "-m: number of payload columns in R\n";
    cout << "-n: number of payload columns in S\n";
    cout << "-t: join type, pkfk or fkfk. Default: pkfk\n";
    cout << "-d: distribution type, uniform or zipf. Default: uniform\n";
    cout << "-z: zipf factor, only valid when -d zipf is used\n";
    cout << "-o: output file name\n";
    cout << "-f: path to the generated data directory if any; otherwise provide a location where you want the generated data to be stored\n";
    cout << "-e: selectivity, only valid when -t pkfk is used. Default: 1.\n";
    cout << "-u: number of unique keys, only valid when -t fkfk is used\n";
    cout << "-i: join algorithm, phj, shj, smj, smji (case sensitive)\n";
    cout << "-p: log_2(partitions in 1st pass) for PHJ. Default: 9.\n";
    cout << "-q: log_2(partitions in 2nd pass) for PHJ. Default: 6.\n";
    cout << "(Note: -p and -q are only valid when -i phj or -i shj is used)\n";
    cout << "Example: ./bin/volcano/join_exp -l -r 12 -s 12 -m 1 -n 1 -t pkfk -d uniform -o join_exp.csv -f /home/data/ -e 1 -i phj -p 9 -q 6\n";
}

void parse_args(int argc, char** argv, struct join_args& args) {
    bool use_log_scale = false;
    for(;;)
    {
      switch(getopt(argc, argv, "r:s:v:m:n:t:d:z:o:e:u:i:p:q:f:alh"))
      {
        case 'r':
            args.nr = atoi(optarg);
            continue;
        case 's':
            args.ns = atoi(optarg);
            continue;
        case 'v':
            args.vec_size = atoi(optarg);
            continue;
        case 'm':
            args.pr = atoi(optarg);
            continue;
        case 'n':
            args.ps = atoi(optarg);
            continue;
        case 't':
            if(strcasecmp(optarg, "fkfk") == 0) {
                args.type = FK_FK;
            }
            continue;
        case 'd':
            if(strcasecmp(optarg, "zipf") == 0) {
                args.dist = ZIPF;
            }
            else {
                args.zipf_factor = 0.0f;
            }
            continue;
        case 'z':
            args.zipf_factor = atof(optarg);
            continue;
        case 'o':
            args.output = std::string(optarg);
            continue;
        case 'f':
            args.data_path_prefix = std::string(optarg);
            if(args.data_path_prefix.back() != '/') args.data_path_prefix += "/";
            continue;
        case 'e':
            args.selectivity = atoi(optarg);
            continue;
        case 'u':
            args.unique_keys = atoi(optarg);
            continue;
        case 'a':
            args.agg_only = true;
            continue;
        case 'l':
            use_log_scale = true;
            continue;
        case 'i':
            if(std::string(optarg) == "phj") args.algo = PHJ;
            else if(std::string(optarg) == "shj") args.algo = SHJ;
            else if(std::string(optarg) == "smji") args.algo = SMJI;
            else args.algo = SMJ;
            continue;
        case 'p':
            args.phj_log_part1 = atoi(optarg);
            continue;
        case 'q':
            args.phj_log_part2 = atoi(optarg);
            continue;
        case 'h':
            print_usage();
            exit(0);

        default :
          printf("[Invalid Input]\n Use -h for help\n");
          break;

        case -1:
          break;
      }

      break;
    }

    if(use_log_scale) {
        args.nr = (1 << args.nr);
        args.ns = (1 << args.ns);
        args.unique_keys = (1 << args.unique_keys);
    }

    args.check();
    args.print();
}

template<typename TupleR, typename TupleS, typename TupleOut>
void free_tuple_mem(TupleR r, TupleS s, TupleOut out){
    r.free_mem();
    s.free_mem();
    out.free_mem();
}


void prepare_running(int argc, char** argv) {
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

    DECL_TUP_1_TO_8(join_key_t, col_t)

    struct join_args args;
    parse_args(argc, argv, args);
}

void say_hello(){
    cout << "hello \n";
}