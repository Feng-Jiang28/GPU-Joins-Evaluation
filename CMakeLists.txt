# Copyright (c) 2020-2024, NVIDIA CORPORATION.

cmake_minimum_required(VERSION 3.26.4)

project(
        basic_example
        VERSION 0.0.1
        LANGUAGES CXX CUDA
)
set(SM_DEF "-DSM700")

# Find OpenMP package
find_package(OpenMP REQUIRED)

include(set_cuda_architecture.cmake)

# initialize cuda architecture
rapids_cuda_init_architectures(basic_example)
rapids_cuda_set_architectures(RAPIDS)

include(fetch_dependencies.cmake)

# Expand CUDF_CUDA_FLAGS
list(APPEND CUDF_CUDA_FLAGS
        --expt-extended-lambda
        --expt-relaxed-constexpr
        --extended-lambda
        -Xptxas="-v"
        -lineinfo
)

# Add CUDA and C++ source files separately
set(CUDA_SOURCES

        src/volcano/inner_join_tables.cpp
)

# Create C++ object library
add_executable(basic_example ${CUDA_SOURCES})
target_link_libraries(basic_example PRIVATE cudf::cudf OpenMP::OpenMP_CXX)

target_compile_options(basic_example PRIVATE
        "$<$<COMPILE_LANGUAGE:CUDA>:${CUDF_CUDA_FLAGS}>"
        "$<$<COMPILE_LANGUAGE:CXX>:-O3>"
        "$<$<COMPILE_LANGUAGE:CUDA>:-O3>"
)

target_compile_definitions(basic_example PRIVATE ${SM_DEF})
target_compile_features(basic_example PRIVATE cxx_std_17)

install(TARGETS basic_example DESTINATION bin/examples/libcudf)