#include <cudf/table/table.hpp>
//#include "cudf_join.hpp"
//#include "../volcano/join_base.hpp"
#include <iostream>

using namespace std;
/*
template<typename TupleR,
        typename TupleS,
        typename TupleOut>
class CudfJoin : public JoinBase<TupleOut>{
static_assert(TupleR::num_cols >= 2 &&
                  TupleS::num_cols >= 2 &&
                  TupleR::num_cols + TupleS::num_cols == TupleOut::num_cols+1);
public:
    explicit CudfJoin(TupleR& relation_r, TupleS& relation_s)
            : r(relation_r)
            , s(relation_s){} // Constructor

    TupleOut join() override{
        // implementation the cudf join algorithm here
        // relation_r & relation_s convert to tableview

        // inner join return a tableview
        // cudf::table\ convert back to TupleOut

        TupleOut result;
        return result;
    }

    void print_stats() override{

    }

    //std::vector<float> all_stats() override{
        // Return a vector of all relevant statistics
    //}


private:
/*
    template <std::pair<std::unique_ptr<rmm::device_uvector<cudf::size_type>>,
            std::unique_ptr<rmm::device_uvector<cudf::size_type>>> (*join_impl)(
            cudf::table_view const& left_keys,
            cudf::table_view const& right_keys,
            cudf::null_equality compare_nulls,
            rmm::device_async_resource_ref mr),
            cudf::out_of_bounds_policy oob_policy = cudf::out_of_bounds_policy::DONT_CHECK>
                                                    std::unique_ptr<cudf::table> join_and_gather(
            cudf::table_view const& left_input,
    cudf::table_view const& right_input,
            std::vector<cudf::size_type> const& left_on,
    std::vector<cudf::size_type> const& right_on,
            cudf::null_equality compare_nulls,
    rmm::device_async_resource_ref mr = rmm::mr::get_current_device_resource())
    {
        auto left_selected  = left_input.select(left_on);
        auto right_selected = right_input.select(right_on);
        auto const [left_join_indices, right_join_indices] =
                join_impl(left_selected, right_selected, compare_nulls, mr);

        auto left_indices_span  = cudf::device_span<cudf::size_type const>{*left_join_indices};
        auto right_indices_span = cudf::device_span<cudf::size_type const>{*right_join_indices};
        auto left_indices_col  = cudf::column_view{left_indices_span};
        auto right_indices_col = cudf::column_view{right_indices_span};

        auto left_result  = cudf::gather(left_input, left_indices_col, oob_policy);
        auto right_result = cudf::gather(right_input, right_indices_col, oob_policy);

        auto joined_cols = left_result->release();
        auto right_cols  = right_result->release();
        joined_cols.insert(joined_cols.end(),
                           std::make_move_iterator(right_cols.begin()),
                           std::make_move_iterator(right_cols.end()));
        return std::make_unique<cudf::table>(std::move(joined_cols));
    }

    std::unique_ptr<cudf::table> inner_join(
            cudf::table_view const& left_input,
            cudf::table_view const& right_input,
            std::vector<cudf::size_type> const& left_on,
            std::vector<cudf::size_type> const& right_on,
            cudf::null_equality compare_nulls = cudf::null_equality::EQUAL)
            {
        return join_and_gather<cudf::inner_join>(
                left_input, right_input, left_on, right_on, compare_nulls);
    }
*/
/*
private:
    static constexpr auto r_cols = TupleR::num_cols;
    static constexpr auto s_cols = TupleS::num_cols;

    using key_t = std::tuple_element_t<0, typename TupleR::value_type>; // key's type to join on
    using r_val_t = std::tuple_element_t<1, typename TupleR::value_type>; // value's type of r
    using s_val_t = std::tuple_element_t<1, typename TupleS::value_type>; // value's type of s

    TupleR r;
    TupleS s;
    TupleOut out;

    int nr;
    int ns;
    int n_matches;

    // Helper function to convert Chunk to cudf::table (as described in the previous response)
    template<typename Tuple>
    cudf::table convert_tuple_to_cudf_table(const Tuple& r){
        std::vector<std::unique_ptr<cudf::column>> columns;

//        // Helper function to create a cudf column from a chunk column
//        auto create_cudf_column = [&](auto i){
//            // get the ith type
//            using col_type = std::tuple_element_t<i.value, typename Tuple::value_type>;
//            auto col_ptr = COL(r, i.value);
//
//            // Create cud::column from the data
//            // Need to specify the correct cudf::data_type for each column
//            auto column = std::make_unique<cudf::column>(
//                    cudf::data_type{cudf::type_to_id<col_type>()},
//                    r.num_items,
//                    rmm::device_buffer{col_ptr, r.num_items * sizeof(col_type)}
//            );
            // Copy data to the column
//            columns.push_back(std::move(column));
//        };
//
//        // Create a column for each element in the chunk
//        for_<Tuple::num_cols>>([&](auto i){
//            create_cudf_column(i);
//        });
        // Create and return th cudf table
        return cudf::table(std::move(columns));
    }

    // Helper function to convert cudf::table back to Tuple (TupleOut)
    template<typename TupleType>
    TupleType convert_cudf_table_to_tuple(const cudf::table& table){
        TupleType result;
      return result;
    }
};


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

template<typename join_key_t, typename col_t, typename TupleR, typename TupleS, typename ResultTuple>
void run_rest_multicols(const struct join_args& args) {
    TupleR relation_r;
    TupleS relation_s;

    prepare_workload<join_key_t, col_t>(args, relation_r, relation_s);

    JoinBase<ResultTuple>* impl;
    auto out = exec_join(relation_r, relation_s, args, impl);
    cout << "\nOutput Cardinality = " << out.num_items << endl;
    cout << "Results (first 10 items): \n";
    out.peek(args.agg_only ? 1 : min(10, out.num_items));
    exp_stats(impl, args);

}
*/
