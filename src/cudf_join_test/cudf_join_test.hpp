#pragma once
#include <iostream>
#include <cudf/column/column.hpp>

// cudf hash inner join function

// read from disk function


int main(int argc, char const** argv) {
    if (argc < 2){
        std::cout << "required parameter: file path \n";
        return 1;
    }

    auto const talbe_path_r = std::string(argv[0]);
    auto const table_path_s = std::string(argv[1]);

    cout << table_path_r << " : " << table_path_s << "\n";

    return 0;
}