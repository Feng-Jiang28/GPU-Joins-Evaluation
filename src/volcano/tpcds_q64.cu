#define CUB_STDERR

#include <iostream>
#include <vector>
#include <tuple>
#include <chrono>
#include <unistd.h>
#include <fstream>

#include <cuda.h>

#include "tuple.cuh"
#include "utils.cuh"
#include "operators.cuh"
#include "tpc_utils.hpp"
#include "../data_gen/generator.cuh"
#include "sort_merge_join.cuh"
#include "partitioned_hash_join.cuh"
#include "sort_hash_join.cuh"
#include "experiment_util.cuh"
#include "join_base.hpp"

using namespace std;

/**
The code is designed to run a specific query (referred to as "q64") using different join algorithms on two data tables.
It loads the data, performs the join operation using the specified algorithm, and then outputs the results and performance statistics.

Main Steps
Load the Data: Load columns from binary files.
Initialize and Open Scans: Set up scan operators to read the data.
Perform the Join: Use the specified join algorithm to combine the tables.
Output Results: Print the result and performance statistics.
Profile and Cleanup: Save the performance statistics to a file and free memory.
*/

// Define types for the columns
#define PAYLOAD_COL_T long
#define KEY_COL_T long

// Macro to load a column from a binary file
#define LOAD_Q64_COL(dir, p, N, from, to, seed) \
    to* p; \
    GET_DATA_TYPE(p) \
    { \
        std::string filename = dir + "/" + #p + ".bin"; \
        read_col<from,to>(filename, p, (N), true, (seed)); \
    }

// Macro to load all required columns
#define LOAD_Q64_COL_ALL(dir, Nss, Ncd) \
    LOAD_Q64_COL((dir), ss_cdemo_sk, (Nss), int, KEY_COL_T, 42) \
    LOAD_Q64_COL((dir), ss_customer_sk, (Nss), long, KEY_COL_T, 42) \
    LOAD_Q64_COL((dir), ss_addr_sk, (Nss), long, KEY_COL_T, 42) \
    LOAD_Q64_COL((dir), ss_item_sk, (Nss), long, KEY_COL_T, 42) \
    LOAD_Q64_COL((dir), ss_ticket_number, (Nss), long, PAYLOAD_COL_T, 42) \
    LOAD_Q64_COL((dir), ss_wholesale_cost, (Nss), long, PAYLOAD_COL_T, 42) \
    LOAD_Q64_COL((dir), ss_list_price, (Nss), long, PAYLOAD_COL_T, 42) \
    LOAD_Q64_COL((dir), ss_coupon_amt, (Nss), long, PAYLOAD_COL_T, 42) \
    LOAD_Q64_COL((dir), d_year, (Nss), long, PAYLOAD_COL_T, 42) \
    LOAD_Q64_COL((dir), s_store_name, (Nss), long, PAYLOAD_COL_T, 42) \
    LOAD_Q64_COL((dir), s_zip, (Nss), long, PAYLOAD_COL_T, 42) \
    LOAD_Q64_COL((dir), cd_demo_sk, (Ncd), int, KEY_COL_T, 42) \
    LOAD_Q64_COL((dir), cd_marital_status, (Ncd), long, PAYLOAD_COL_T, 42)

// Function to run the q64 query with a specified algorithm
void run_q64(std::string algo, std::string profile_output_) {
    const int Nss = 57898426;
    const int Ncd = 1920800;
    std::string dir(TPC_DATA_PREFIX"tpcds_sf100/q64");

    // Load all columns
    LOAD_Q64_COL_ALL(dir, Nss, Ncd)

    // Define the chunk types for the tables and the join result
    using ss_t = struct Chunk<ss_cdemo_sk_t,ss_customer_sk_t,ss_addr_sk_t,ss_item_sk_t,ss_ticket_number_t,ss_wholesale_cost_t,ss_list_price_t,ss_coupon_amt_t,d_year_t,s_store_name_t,s_zip_t>;
    using cd_t = struct Chunk<cd_demo_sk_t, cd_marital_status_t>;
    
    using join_t = struct Chunk<cd_demo_sk_t,cd_marital_status_t,ss_customer_sk_t,ss_addr_sk_t,ss_item_sk_t,ss_ticket_number_t,ss_wholesale_cost_t,ss_list_price_t,ss_coupon_amt_t,d_year_t,s_store_name_t,s_zip_t>;

    // Create tuples from the columns
    auto cd_cols = std::make_tuple(cd_demo_sk, cd_marital_status);
    auto ss_cols = std::make_tuple(ss_cdemo_sk,ss_customer_sk,ss_addr_sk,ss_item_sk,ss_ticket_number,ss_wholesale_cost,ss_list_price,ss_coupon_amt,d_year,s_store_name,s_zip);

    // Initialize scan operators to read the data
    ScanOperator<cd_t> cd_scan(std::move(cd_cols), Ncd, Ncd);
    ScanOperator<ss_t> ss_scan(std::move(ss_cols), Nss, Nss);

    // Open scans and read data
    cd_scan.open(); ss_scan.open();
    auto relation_cd = cd_scan.next();
    auto relation_ss = ss_scan.next();
    cd_scan.close(); ss_scan.close();

    const int circular_buffer_size = Nss;  // Size of the circular buffer
    const int first_bit = 0;
    const int log_part1 = 9;
    const int log_part2 = 6;

    // Pointer to the join implementation
    JoinBase<join_t> *impl;
    // Select the join algorithm based on the input argument
    if (algo == "SMJ") {
        impl = new SortMergeJoin<cd_t, ss_t, join_t, true>(relation_cd, relation_ss, circular_buffer_size);
    } else if (algo == "PHJ") {
        impl = new PartitionHashJoin<cd_t, ss_t, join_t>(relation_cd, relation_ss, log_part1, log_part2, first_bit, circular_buffer_size);
    } else if (algo == "SHJ") {
        impl = new SortHashJoin<cd_t, ss_t, join_t>(relation_cd, relation_ss, first_bit, log_part1+log_part2, circular_buffer_size);
    } else if (algo == "SMJI") {
        impl = new SortMergeJoinByIndex<cd_t, ss_t, join_t>(relation_cd, relation_ss, circular_buffer_size);
    } 
    else {
        std::cout << "Invalid algorithm name: " << algo << std::endl;
        exit(1);
    }

    // Perform the join operation
    auto out = impl->join();
    cudaDeviceSynchronize();

    cout << "\nOutput Cardinality = " << out.num_items << endl;
    cout << "Results (first 10 items): \n";
    out.peek(min(10, out.num_items));

    // Print performance statistics
    impl->print_stats();

    // Save performance statistics to a file
    std::ofstream fout;
    fout.open(profile_output_, ios::app);
    fout << get_utc_time() << ","
        << relation_cd.num_items << "," << relation_ss.num_items << ","
        << algo << ",";

    auto stats = impl->all_stats();
    for(auto t : stats) {
        fout << t << ",";
    }

    fout << std::endl;
    fout.close();

    // Free memory
    relation_cd.free_mem();
    relation_ss.free_mem();
    out.free_mem();
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cout << "Usage: ./bin/volcano/q64 <algo> <profile_output>" << std::endl;
        exit(1);
    }
    std::string algo = argv[1]; // Join algorithm to use
    std::string profile_output = argv[2]; // Output file for performance statistics
    run_q64(algo, profile_output); // Run the query
    return 0;
}