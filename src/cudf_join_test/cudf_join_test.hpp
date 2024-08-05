#pragma once

#include <cudf/io/csv.hpp>
#include <cudf/io/datasource.hpp>

#include <rmm/cuda_device.hpp>
#include <rmm/mr/device/cuda_memory_resource.hpp>
#include <rmm/mr/device/device_memory_resource.hpp>
#include <rmm/mr/device/owning_wrapper.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>

#include <chrono>
#include <iostream>
#include <memory>
#include <string>

using namespace std;
// cudf hash inner join function

// read from disk function

/**
 * @brief Create CUDA memory resource
 */
auto make_cuda_mr() { return std::make_shared<rmm::mr::cuda_memory_resource>(); }

/**
 * @brief Create a pool device memory resource
 */
auto make_pool_mr()
{
    return rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(
      make_cuda_mr(), rmm::percent_of_free_device_memory(50));
}

/**
 * @brief Create memory resource for libcudf functions
 */
std::shared_ptr<rmm::mr::device_memory_resource> create_memory_resource(std::string const& name)
{
    if (name == "pool") { return make_pool_mr(); }
    return make_cuda_mr();
}


int main(int argc, char const** argv) {
    if (argc < 3){
        cout << "required parameter: file path \n";
        return 1;
    }

    auto const table_path_r = string(argv[1]);
    auto const table_path_s = string(argv[2]);

    cout << table_path_r << " : " << table_path_s << "\n";

    auto const mr_name = string("cuda");

    auto resource = create_memory_resource(mr_name);
    rmm::mr::set_current_device_resource(resouce.get());

    auto r_result = [table_path_r]{
        cudf::io::csv_reader_options in_opts =
            cudf::io::csv_reader_options::builder(cudf::io::source_info{table_path_r}).header(-1);
        return cudf::io::read_csv(in_opts).tbl;
    }();



    return 0;
}