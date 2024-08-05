#pragma once

#include <cudf/io/csv.hpp>
#include <cudf/io/datasource.hpp>

#include <rmm/cuda_device.hpp>
#include <rmm/mr/device/cuda_memory_resource.hpp>
#include <rmm/mr/device/device_memory_resource.hpp>
#include <rmm/mr/device/owning_wrapper.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>

#include <iostream>
#include <string>

using namespace std;
// cudf hash inner join function

// read from disk function


int main(int argc, char const** argv) {
    if (argc < 3){
        cout << "required parameter: file path \n";
        return 1;
    }

    auto const table_path_r = string(argv[1]);
    auto const table_path_s = string(argv[2]);

    cout << table_path_r << " : " << table_path_s << "\n";

    auto resource = create_memory_resource("cuda");
    rmm::mr::set_current_device_resource(resouce.get());

    auto r_result = [table_path_r]{
        cudf::io::csv_reader_options in_opts =
            cudf::io::csv_reader_options::builder(cudf::io::source_info{csv_file}).header(-1);
        return cudf::io::read_csv(in_opts).tbl;
    }();



    return 0;
}