#pragma once
#include <iostream>
#include <string>
#include <cudf/column/column.hpp>

using namespace std;
// cudf hash inner join function

// read from disk function


int main(int argc, char const** argv) {
    if (argc < 3){
        cout << "required parameter: file path \n";
        return 1;
    }

    auto const table_path_r = argv[1];
    auto const table_path_s = string(argv[2]);

    cout << table_path_r << " : " << table_path_s << "\n";

    return 0;
}