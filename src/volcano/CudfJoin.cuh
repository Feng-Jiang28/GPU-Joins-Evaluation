#include <cudf/table/table_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/join.hpp>
#include <cudf/io/csv.hpp>
#include <rmm/device_buffer.hpp>
#include <memory>
#include <iostream>

template<typename T>
class CudfJoin : public JoinBase<T> {
private:
    cudf::table_view relation_r;
    cudf::table_view relation_s;

public:
    CudfJoin(cudf::table_view r, cudf::table_view s) : relation_r(r), relation_s(s) {}

    T join() override {
        // Assuming the join keys are the first columns in each table
        std::vector<cudf::size_type> left_on{0};
        std::vector<cudf::size_type> right_on{0};

        // Perform inner join
        auto result = cudf::inner_join(relation_r, relation_s, left_on, right_on);

        return result;
    }

    void print_stats() override {
        std::cout << "Join completed using cuDF" << std::endl;
    }

    std::vector<float> all_stats() override {
        return {};
    }

    ~CudfJoin() override = default;
};