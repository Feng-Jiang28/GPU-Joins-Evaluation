#pragma once
#define CUB_STDERR

#include <iostream>
#include <stdio.h>
#include <curand.h>
#include <vector>
#include <tuple>
#include <string>
#include <type_traits>
#include <algorithm>

#include <cuda.h>
#include <cub/cub.cuh> 

#include "tuple.cuh"
#include "utils.cuh"
#include "partition_util.cuh"
#include "phj_util.cuh"
#include "join_base.hpp"
#include <thrust/gather.h>
#include <thrust/device_ptr.h>

template<typename TupleR,
         typename TupleS,
         typename TupleOut,
         bool     kAlwaysLateMaterialization = false>
class PartitionHashJoin : public JoinBase<TupleOut> {
    static_assert(TupleR::num_cols >= 2 && 
                  TupleS::num_cols >= 2 && 
                  TupleR::num_cols+TupleS::num_cols == TupleOut::num_cols+1);
                  
public:
    // r_in: The first input table.
    // s_in: The second input table.
    // log_parts1: log base 2 of # of partitions for first level partitioning
    // log_parts2: log base 2 of # of partitions for second level partitioning
    // first_bit: starting bit for partitioning
    explicit PartitionHashJoin(TupleR r_in, TupleS s_in, int log_parts1, int log_parts2, int first_bit, int circular_buffer_size) 
             : r(r_in) // Initialize r with r_in
             , s(s_in) // Initialize s with s_in
             , log_parts1(log_parts1) // Initialize log_parts1
             , log_parts2(log_parts2)  // Initialize log_parts2
             , first_bit(first_bit)  // Initialize first_bit
             , circular_buffer_size(circular_buffer_size) { // Initialize circular_buffer_size
        // Number of items in r and s
        nr = r.num_items;
        ns = s.num_items;

        // Allocate memory for the output tuple
        out.allocate(circular_buffer_size);

        // Calculate the number of partitions
        parts1 = 1 << log_parts1;
        parts2 = 1 << (log_parts1 + log_parts2);

        // Calculate the maximum number of buckets needed for r and s
        buckets_num_max_R    = ((((nr + parts2 - 1)/parts2) + bucket_size - 1)/bucket_size)*parts2 + 1024;

        // we consider two extreme cases here
        // (1) S keys are uniformly distributed across partitions; (2) S keys concentrate in one partition
        buckets_num_max_S    = std::max(((((ns + parts2 - 1)/parts2) + bucket_size - 1)/bucket_size)*parts2, 
                                        (ns+bucket_size-1)/bucket_size+parts2);

        // Allocate memory for key partitions of r and s
        allocate_mem(&r_key_partitions, true, buckets_num_max_R * bucket_size * sizeof(key_t));
        allocate_mem(&s_key_partitions, true, buckets_num_max_S * bucket_size * sizeof(key_t));

        // Copy keys from r and s to then allocated partitions
        cudaMemcpy(r_key_partitions, COL(r,0), nr*sizeof(key_t), cudaMemcpyDefault);
        cudaMemcpy(s_key_partitions, COL(s,0), ns*sizeof(key_t), cudaMemcpyDefault);
//  #ifndef CHECK_CORRECTNESS
//          release_mem(COL(r,0));
//          release_mem(COL(s,0));
//  #endif
        // Allocate temporary memory for key partitions of r and s
        allocate_mem(&r_key_partitions_temp, true, buckets_num_max_R * bucket_size * sizeof(key_t));
        allocate_mem(&s_key_partitions_temp, true, buckets_num_max_S * bucket_size * sizeof(key_t));

        if constexpr (early_materialization) { // If early materialization is enabled
            // Allocate memory for value partitions of r and s
            allocate_mem(&r_val_partitions, true, buckets_num_max_R * bucket_size * sizeof(r_val_t));
            allocate_mem(&s_val_partitions, true, buckets_num_max_S * bucket_size * sizeof(s_val_t));

            // Copy values from r and s to the allocated partitions
            cudaMemcpy((r_val_t*)(r_val_partitions), COL(r,1), nr*sizeof(r_val_t), cudaMemcpyDefault);
            cudaMemcpy((s_val_t*)(s_val_partitions), COL(s,1), ns*sizeof(s_val_t), cudaMemcpyDefault);
//  #ifndef CHECK_CORRECTNESS
//              release_mem(COL(r,1));
//              release_mem(COL(s,1));
//  #endif
            // allocated temporary memory for value partitions of r and s
            allocate_mem(&s_val_partitions_temp, true, buckets_num_max_S * bucket_size * sizeof(s_val_t));
            allocate_mem(&r_val_partitions_temp, true, buckets_num_max_R * bucket_size * sizeof(r_val_t));
        }
        else { // If early materialization is not enabled
            // Allocate memory for value partitions of r and s as int32_t
            allocate_mem(&r_val_partitions, true, buckets_num_max_R * bucket_size * sizeof(int32_t));
            allocate_mem(&r_val_partitions_temp, true, buckets_num_max_R * bucket_size * sizeof(int32_t));
            allocate_mem(&s_val_partitions, true, buckets_num_max_S * bucket_size * sizeof(int32_t));
            allocate_mem(&s_val_partitions_temp, true, buckets_num_max_S * bucket_size * sizeof(int32_t));

            // Allocate memory for match indices
            allocate_mem(&r_match_idx, false, sizeof(int)*circular_buffer_size);
            allocate_mem(&s_match_idx, false, sizeof(int)*circular_buffer_size);

            // Fill sequence for value partitions of r and s
            fill_sequence<<<num_tb(nr), 1024>>>((int*)(r_val_partitions), 0, nr);
            fill_sequence<<<num_tb(ns), 1024>>>((int*)(s_val_partitions), 0, ns);
        }

        for (int i = 0; i < 2; i++) {
            // Allocate memory for chains, counts, heads, and buckets used for a and s
            allocate_mem(&chains_R[i], false, buckets_num_max_R * sizeof(uint32_t));
            allocate_mem(&cnts_R[i], false, parts2 * sizeof(uint32_t));
            allocate_mem(&heads_R[i], false, parts2 * sizeof(uint64_t));
            allocate_mem(&buckets_used_R[i], false, sizeof(uint32_t));

            allocate_mem(&chains_S[i], false, buckets_num_max_S * sizeof(uint32_t));
            allocate_mem(&cnts_S[i], false, parts2 * sizeof(uint32_t));
            allocate_mem(&heads_S[i], false, parts2 * sizeof(uint64_t));
            allocate_mem(&buckets_used_S[i], false, sizeof(uint32_t));
        }

        // Set bucket_info_R to point to s_val_partitions_temp
        bucket_info_R = (uint32_t*)s_val_partitions_temp;

        // Allocate memory for number of matches
        allocate_mem(&d_n_matches);

        // Create CUDA events for timing
        cudaEventCreate(&start); 
        cudaEventCreate(&stop);
    }

    ~PartitionHashJoin() {
        release_mem(r_key_partitions);
        release_mem(r_key_partitions_temp);
        release_mem(s_key_partitions);
        release_mem(s_key_partitions_temp);
        release_mem(r_val_partitions);
        release_mem(r_val_partitions_temp);
        release_mem(s_val_partitions);
        release_mem(s_val_partitions_temp);
        for (int i = 0; i < 2; i++) {
            release_mem(chains_R[i]);
            release_mem(cnts_R[i]);
            release_mem(heads_R[i]);
            release_mem(buckets_used_R[i]);

            release_mem(chains_S[i]);
            release_mem(cnts_S[i]);
            release_mem(heads_S[i]);
            release_mem(buckets_used_S[i]);
        }
        release_mem(d_n_matches);
        if constexpr (!early_materialization) {
            release_mem(r_match_idx);
            release_mem(s_match_idx);
        }

        cudaEventDestroy(start);
        cudaEventDestroy(stop);
    }

public:
    // This method performs the entire join process and returns the resulting joined tuple.
    TupleOut join() override {
        // Measure and accumulate the time taken for the partitioning step.
        TIME_FUNC_ACC(partition(), partition_time);

        // Swap the roles of R and S to ensure balanced partitioning.
        swap_r_s();

        // Measure and accumulate the time taken for balancing the buckets.
        TIME_FUNC_ACC(balance_buckets(), partition_time);

        // Measure and accumulate the time taken for performing the hash join.
        TIME_FUNC_ACC(hash_join(), join_time);

        // Swap the roles of R and S back to their original roles?
        swap_r_s();

        // Measure and accumulate the time taken for materializing the result.
        TIME_FUNC_ACC(materialize_by_gather(), mat_time);

        // If early materialization is enabled, swap data columns for the
        if constexpr (early_materialization) {
            std::swap(out.data[1], out.data[2]);
        }

        // Set the number of items in the output tuple to the number of matches found.
        out.num_items = n_matches;


        // Return the resulting joined tuple.
        return out;
    }

    // This method prints the timing statistics for the join operation.
    void print_stats() override {
        std::cout << "Partition: " << partition_time << " ms\n"
                  << "Join: " << join_time << " ms\n"
                  << "Materialize: " << mat_time << " ms\n\n";
    }

    // This method returns the timing statistics for the join operation as a vector of floats.
    std::vector<float> all_stats() override {
        return {partition_time, join_time, mat_time};
    }

public:
    // Timing variables to store the time taken for each step of the join process
    float partition_time {0};
    float join_time {0};
    float mat_time {0};

private:
    // Template function for partitioning the keys and values
    template<typename KeyT, typename val_t>
    void partition(KeyT* keys, KeyT* keys_out, 
                   val_t* vals, val_t* vals_out, int n, int buckets_num,
                   uint64_t* heads[2], uint32_t* cnts[2], 
                   uint32_t* chains[2], uint32_t* buckets_used[2]) {
    // Determine the number of threads per block (NT) based on the size of the key type
    constexpr int NT = (sizeof(KeyT) == 4 ? 1024 : 512);
    // Number of values processed per thread (VT)
    constexpr int VT = 4;

    // shared memory size calculation:
    // Calculate the size of shard memory needed for the first and second partition passes.
    // p1_sm_bytes: shared memory for the shuffle region, histogram region, and extra info for the first pass
    const size_t p1_sm_bytes = (NT*VT) * max(sizeof(KeyT), sizeof(val_t)) + (4*(1 << log_parts1)) * sizeof(int32_t);
    // p2_sm_bytes: shared memory for the shuffle region, histogram region, and extra meta info for the second pass
    const size_t p2_sm_bytes = (NT*VT) * max(sizeof(KeyT), sizeof(val_t)) + (4*(1 << log_parts2)) * sizeof(int32_t);
    
#if defined(SM860)
    const int sm_counts = 82;
#elif defined(SM800)
    const int sm_counts = 108;
#endif

    // Initialize metadata for double buffering
    init_metadata_double<<<sm_counts, NT, 0>>> (
        heads[0], buckets_used[0], chains[0], cnts[0], 
        1 << log_parts1, buckets_num,
        heads[1], buckets_used[1], chains[1], cnts[1], 
        1 << (log_parts1 + log_parts2), buckets_num, 
        bucket_size
    );

    // First partition pass: partition the keys and values into initial buckets.
    partition_pass_one<NT, VT><<<sm_counts, NT, p1_sm_bytes>>>(
                                                keys, 
                                                vals,
                                                heads[0],
                                                buckets_used[0],
                                                chains[0],
                                                cnts[0],
                                                keys_out, 
                                                vals_out,
                                                n,
                                                log_parts1,
                                                first_bit + log_parts2,
                                                log2_bucket_size);

    // Compute bucket information based on the results of the first partition pass
    compute_bucket_info <<<sm_counts, NT>>> (chains[0], cnts[0], log_parts1);

    // Second partition pass: refine the partitioning into finer buckets
    partition_pass_two<NT, VT> <<<sm_counts, NT, p2_sm_bytes>>>(
                                    keys_out, 
                                    vals_out,
                                    chains[0],
                                    buckets_used[1], 
                                    heads[1], 
                                    chains[1], 
                                    cnts[1],
                                    keys, 
                                    vals,
                                    log_parts2, 
                                    first_bit,
                                    buckets_used[0],
                                    log2_bucket_size);
}
    // Function to partition the input keys and values into buckets
    void partition() {
        // Define temporary types for r and s based on whether early materialization is enabled
        using r_temp_t = std::conditional_t<early_materialization, r_val_t*, int*>;
        using s_temp_t = std::conditional_t<early_materialization, s_val_t*, int*>;

        // Partition the keys and values for relation R
        partition(r_key_partitions, r_key_partitions_temp, 
                  (r_temp_t)(r_val_partitions), (r_temp_t)(r_val_partitions_temp), 
                  nr, 
                  buckets_num_max_R, heads_R, cnts_R, chains_R, buckets_used_R);

        // Partition the keys and values for relation S
        partition(s_key_partitions, s_key_partitions_temp, 
                  (s_temp_t)(s_val_partitions), (s_temp_t)(s_val_partitions_temp), 
                  ns, 
                  buckets_num_max_S, heads_S, cnts_S, chains_S, buckets_used_S);
    }
    // Function to balance the buckets after partitioning
    void balance_buckets() {
        // Decompose chains to balance the bucket sizes
        decompose_chains <<<(1 << log_parts1), 1024>>> (
        bucket_info_R, // Bucket information
        chains_R[1], // Chains for R in the second pass
        cnts_R[1], // Counts for R in the second pass
        log_parts1 + log_parts2, // Total number of partitioning bits
        2*bucket_size, // Twice the bucket size
        bucket_size // Bucket size
        );
    }

    // Function to swap the roles of R and S
    void swap_r_s() {
        // Swap the key partition of R and S
        std::swap(r_key_partitions, s_key_partitions);
        // Swap the value partitions of R and S
        std::swap(r_val_partitions, s_val_partitions);
        // Swap the match indices of R and S
        std::swap(r_match_idx, s_match_idx);

        // Swap the chains, counts, heads, and buckets used for R and S
        for (int i = 0; i < 2; i++) {
            std::swap(chains_R[i], chains_S[i]);
            std::swap(cnts_R[i], cnts_S[i]);
            std::swap(heads_R[i], heads_S[i]);
            std::swap(buckets_used_R[i], buckets_used_S[i]);
        }
    }

    void hash_join() {
        constexpr int NT = 512;
        constexpr int VT = 4;

        if constexpr (early_materialization) {
            size_t sm_bytes = (bucket_size + 512) * (sizeof(key_t) + sizeof(r_val_t) + sizeof(int16_t)) + // elem, payload and next resp.
                            (1 << LOCAL_BUCKETS_BITS) * sizeof(int32_t) + // hash table head
                            + SHUFFLE_SIZE * (NT/32) * (sizeof(key_t) + sizeof(r_val_t) + sizeof(s_val_t));
            std::cout << "sm_bytes: " << sm_bytes << std::endl;
            auto join_fn = join_copartitions<NT, VT, LOCAL_BUCKETS_BITS, SHUFFLE_SIZE, key_t, r_val_t>;
            cudaFuncSetAttribute(join_fn, cudaFuncAttributeMaxDynamicSharedMemorySize, sm_bytes);
            join_fn<<<(1 << (log_parts1+log_parts2)), NT, sm_bytes>>>
                                (r_key_partitions, (r_val_t*)(r_val_partitions), 
                                chains_R[1], bucket_info_R, 
                                s_key_partitions, (s_val_t*)(s_val_partitions), 
                                cnts_S[1], chains_S[1], log_parts1 + log_parts2, buckets_used_R[1], 
                                bucket_size,
                                d_n_matches,
                                COL(out, 0), COL(out, 1), COL(out, 2), circular_buffer_size);
        }
        else {
            size_t sm_bytes = (bucket_size + 512) * (sizeof(key_t) + sizeof(int) + sizeof(int16_t)) + // elem, payload and next resp.
                            (1 << LOCAL_BUCKETS_BITS) * sizeof(int32_t) + // hash table head
                            + SHUFFLE_SIZE * (NT/32) * (sizeof(key_t) + sizeof(int)*2);
            std::cout << "sm_bytes: " << sm_bytes << std::endl;
            auto join_fn = join_copartitions<NT, VT, LOCAL_BUCKETS_BITS, SHUFFLE_SIZE, key_t, int>;
            cudaFuncSetAttribute(join_fn, cudaFuncAttributeMaxDynamicSharedMemorySize, sm_bytes);
            join_fn<<<(1 << (log_parts1+log_parts2)), NT, sm_bytes>>>
                                (r_key_partitions, (int*)(r_val_partitions), 
                                chains_R[1], bucket_info_R, 
                                s_key_partitions, (int*)(s_val_partitions), 
                                cnts_S[1], chains_S[1], log_parts1 + log_parts2, buckets_used_R[1],
                                bucket_size,
                                d_n_matches, 
                                COL(out, 0), r_match_idx, s_match_idx, circular_buffer_size);
        }

        cudaMemcpy(&n_matches, d_n_matches, sizeof(n_matches), cudaMemcpyDeviceToHost);
    }

    void materialize_by_gather() {
        if constexpr (!early_materialization) {
            for_<r_cols-1>([&](auto i) {
                using val_t = std::tuple_element_t<i.value+1, typename TupleR::value_type>;
                thrust::device_ptr<val_t> dev_data_ptr(COL(r, i.value+1));
                thrust::device_ptr<int> dev_idx_ptr(r_match_idx);
                thrust::device_ptr<val_t> dev_out_ptr(COL(out, i.value+1));
                thrust::gather(dev_idx_ptr, dev_idx_ptr+std::min(circular_buffer_size, n_matches), dev_data_ptr, dev_out_ptr);
            });
            for_<s_cols-1>([&](auto i) {
                constexpr auto k = i.value+r_cols;
                using val_t = std::tuple_element_t<i.value+1, typename TupleS::value_type>;
                
                // using thrust has the same performance
                thrust::device_ptr<val_t> dev_data_ptr(COL(s, i.value+1));
                thrust::device_ptr<int> dev_idx_ptr(s_match_idx);
                thrust::device_ptr<val_t> dev_out_ptr(COL(out, k));
                thrust::gather(dev_idx_ptr, dev_idx_ptr+std::min(circular_buffer_size, n_matches), dev_data_ptr, dev_out_ptr);
            });
        }
    }

private:
    static constexpr auto  r_cols = TupleR::num_cols;
    static constexpr auto  s_cols = TupleS::num_cols;
    static constexpr bool r_materialize_early = (r_cols == 2 && !kAlwaysLateMaterialization);
    static constexpr bool s_materialize_early = (s_cols == 2 && !kAlwaysLateMaterialization);
    static constexpr bool early_materialization = (r_materialize_early && s_materialize_early);
    static constexpr uint32_t log2_bucket_size = 12;
    static constexpr uint32_t bucket_size = (1 << log2_bucket_size);
    static constexpr int LOCAL_BUCKETS_BITS = 11;
    static constexpr int SHUFFLE_SIZE = (early_materialization ? ((TupleR::row_bytes + TupleS::row_bytes <= 24) ? 32 : 24) : (sizeof(std::tuple_element_t<0, typename TupleR::value_type>) == 4 ? 32 : 16));
    
    using key_t = std::tuple_element_t<0, typename TupleR::value_type>;
    using r_val_t = std::tuple_element_t<1, typename TupleR::value_type>;
    using s_val_t = std::tuple_element_t<1, typename TupleS::value_type>;

    TupleR r;
    TupleS s;
    TupleOut out;
    
    int nr;
    int ns;
    int n_matches;
    int circular_buffer_size;

    int first_bit;
    int parts1;
    int parts2;
    int log_parts1;
    int log_parts2;
    size_t buckets_num_max_R;
    size_t buckets_num_max_S;

    key_t* r_key_partitions      {nullptr};
    key_t* s_key_partitions      {nullptr};
    key_t* r_key_partitions_temp {nullptr};
    key_t* s_key_partitions_temp {nullptr};

    void* r_val_partitions       {nullptr};
    void* s_val_partitions       {nullptr};
    void* r_val_partitions_temp  {nullptr};
    void* s_val_partitions_temp  {nullptr};

    // meta info for bucket chaining
    uint32_t* chains_R[2];
    uint32_t* chains_S[2];

    uint32_t* cnts_R[2];
    uint32_t* cnts_S[2];

    uint64_t* heads_R[2];
    uint64_t* heads_S[2];

    uint32_t* buckets_used_R[2];
    uint32_t* buckets_used_S[2];

    uint32_t* bucket_info_R{nullptr};

    int*   r_match_idx     {nullptr};
    int*   s_match_idx     {nullptr};

    int*   d_n_matches     {nullptr};

    cudaEvent_t start;
    cudaEvent_t stop;
};