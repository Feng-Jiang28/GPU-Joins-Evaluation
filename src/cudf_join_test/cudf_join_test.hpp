#pragma once

#include <iostream>
#include <cudf/column/column.hpp>
int main(int argc, char const** argv) {
    if (argc < 2){
        std::cout << "required parameter: file path";
    }
    return 0;
}