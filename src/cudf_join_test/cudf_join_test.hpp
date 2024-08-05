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
std::unique_ptr<cudf::table> inner_join(
    cudf::table_view const& left_input,
     cudf::table_view const& right_input,
     std::vector<cudf::size_type> const& left_on,
     std::vector<cudf::size_type> const& right_on,
     cudf::null_equality compare_nulls );

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
    rmm::mr::set_current_device_resource(resource.get());

    auto r_result = [table_path_r]{
        cudf::io::csv_reader_options in_opts =
            cudf::io::csv_reader_options::builder(cudf::io::source_info{table_path_r}).header(-1);
        return cudf::io::read_csv(in_opts).tbl;
    }();

    auto s_result = [table_path_s]{
        cudf::io::csv_reader_options in_opts =
          cudf::io::csv_reader_options::builder(cudf::io::source_info{table_path_s}).header(-1);
        return cudf::io::read_csv(in_opts).tbl;
    }();

    auto const table_r = r_result -> view();
    std::cout << "table_r: " << table_r.num_rows() << " rows " << table_r.num_columns()
           << " columns\n";
    auto const table_s = s_result -> view();
    std::cout << "table_s: " << table_s.num_rows() << " rows " << table_s.num_columns()
           << " columns\n";

    auto result = inner_join(table_r, table_s, {0, 1, 2}, {0, 1, 2}, cudf::null_equality::EQUAL);

    return 0;
}