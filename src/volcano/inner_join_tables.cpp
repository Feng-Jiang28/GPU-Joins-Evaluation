#include <cudf/column/column.hpp>
#include <cudf/types.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/filling.hpp>
#include <cudf/join.hpp>
#include <cudf_test/testing_main.hpp>
#include <cudf/io/csv.hpp>

#include <rmm/cuda_device.hpp>
#include <rmm/mr/device/cuda_memory_resource.hpp>
#include <rmm/mr/device/device_memory_resource.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>

#include <memory>

void write_csv(cudf::table_view const& tbl_view, std::string const& file_path)
{
  auto sink_info = cudf::io::sink_info(file_path);
  auto builder   = cudf::io::csv_writer_options::builder(sink_info, tbl_view);
  auto options   = builder.build();
  cudf::io::write_csv(options);
}

int main(){

     // Construct a CUDA memory resource using RAPIDS Memory Manager (RMM)
      // This is the default memory resource for libcudf for allocating device memory.
      rmm::mr::cuda_memory_resource cuda_mr{};
      // Construct a memory pool using the CUDA memory resource
      // Using a memory pool for device memory allocations is important for good performance in libcudf.
      // The pool defaults to allocating half of the available GPU memory.
      rmm::mr::pool_memory_resource mr{&cuda_mr, rmm::percent_of_free_device_memory(50)};

      // Set the pool resource to be used by default for all device memory allocations
      // Note: It is the user's responsibility to ensure the `mr` object stays alive for the duration of
      // it being set as the default
      // Also, call this before the first libcudf API call to ensure all data is allocated by the same
      // memory resource.
      rmm::mr::set_current_device_resource(&mr);

    // Initialize the default memory resource

    // Create sample data for the first table
    std::vector<int32_t> col1_data = {1, 2, 3, 4};
    std::vector<int32_t> col2_data = {10, 20, 30, 40};

    std::unique_ptr<cudf::column> col1 = cudf::make_numeric_column(cudf::data_type(cudf::type_id::INT32), col1_data.size());
    std::unique_ptr<cudf::column> col2 = cudf::make_numeric_column(cudf::data_type(cudf::type_id::INT32), col2_data.size());

    auto init  = cudf::make_fixed_width_scalar<int32_t>(static_cast<int32_t>(0));
    auto left_payload_column  = cudf::sequence(4, *init);

    cudf::table_view left_table({col1->view(), col2->view(), *left_payload_column});

    // Create sample data for the second table

     std::vector<int32_t> col3_data = {1, 2, 3, 4};
     std::vector<int32_t> col4_data = {10, 20, 30, 40};

     std::unique_ptr<cudf::column> col3 = cudf::make_numeric_column(cudf::data_type(cudf::type_id::INT32), col1_data.size());
     std::unique_ptr<cudf::column> col4 = cudf::make_numeric_column(cudf::data_type(cudf::type_id::INT32), col2_data.size());

     auto right_payload_column  = cudf::sequence(4, *init);

     cudf::table_view right_table({col3->view(), col4->view(), *right_payload_column});
     // Perform inner join on the first column (key) of both tables
     auto result = cudf::inner_join(left_table, right_table, cudf::null_equality::UNEQUAL);
     // Display the result


     return 0;

}
