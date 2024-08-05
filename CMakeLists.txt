# Copyright (c) 2020-2024, NVIDIA CORPORATION.

cmake_minimum_required(VERSION 3.26.4)

project(
        basic_example
        VERSION 0.0.1
        LANGUAGES CXX CUDA
)
set(SM_DEF "-DSM700")
set(CMAKE_CXX_STANDARD 17)

# Find OpenMP package
find_package(OpenMP REQUIRED)

include(set_cuda_architecture.cmake)

# initialize cuda architecture
rapids_cuda_init_architectures(basic_example)
rapids_cuda_set_architectures(RAPIDS)

include(fetch_dependencies.cmake)

# Expand CUDF_CUDA_FLAGS
list(APPEND CUDA_FLAGS
        --expt-extended-lambda
        --expt-relaxed-constexpr
        --extended-lambda
        -Xptxas="-v"
        -lineinfo
)

list(APPEND CUDF_CUDA_FLAGS --expt-extended-lambda --expt-relaxed-constexpr)

#set(CUDF_SOURCES
#        src/cudf_join/cudf_join_kernel.cu
#        src/cudf_join/cudf_join_impl.cpp
#)

# Add CUDA and C++ source files separately
set(CUDA_SOURCES
        src/cudf_join/cudf_join_kernel.cu
        #src/cudf_join_test/cudf_join_impl.cpp
)

# Create C++ object library
#add_executable(cudf_kernel ${CUDF_SOURCES})
#target_compile_features(cudf_kernel PRIVATE cxx_std_17)
#target_link_libraries(cudf_kernel PRIVATE cudf::cudf)
#target_compile_options(cudf_kernel PRIVATE
#        "$<$<COMPILE_LANGUAGE:CUDA>:${CUDF_CUDA_FLAGS}>"
#)
#target_compile_definitions(cudf_kernel PRIVATE ${SM_DEF})
#target_compile_features(cudf_kernel PRIVATE cxx_std_17)
#install(TARGETS cudf_kernel DESTINATION bin/examples/libcudf)

# Create C++ object library
add_executable(cuda_kernel ${CUDA_SOURCES})
target_link_libraries(cuda_kernel PRIVATE cudf::cudf OpenMP::OpenMP_CXX)
target_compile_options(cuda_kernel PRIVATE
        "$<$<COMPILE_LANGUAGE:CUDA>:${CUDA_FLAGS}>"
        "$<$<COMPILE_LANGUAGE:CXX>:-O3>"
        "$<$<COMPILE_LANGUAGE:CUDA>:-O3>"
)
target_compile_definitions(cuda_kernel PRIVATE ${SM_DEF})
install(TARGETS cuda_kernel DESTINATION bin/examples/libcudf)

