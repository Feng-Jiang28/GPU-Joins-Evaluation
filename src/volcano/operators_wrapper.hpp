#pragma once
#include <iostream>
#include <tuple>
#include "tuple_wrapper.hpp"

class Operator {
public:
    float next_this;
    std::string op_name;
    Operator(std::string&& str) : op_name(str), next_this(0) {}
    virtual float get_op_time() = 0;
};

template<typename Tuple>
class ScanOperator : public Operator {
public:
    ScanOperator(typename Tuple::row_type&& src, size_t num_items, int vec_size);

    virtual void open();
    virtual void close();
    virtual float get_op_time() override;

    Tuple next();

protected:
    virtual Tuple next_();

protected:
    using tuple_row_type = typename Tuple::row_type;
    tuple_row_type in_;
    const size_t num_items_;
    const int num_cols_;
    const int vec_size_;
    size_t start_idx_;
};