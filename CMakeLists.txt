# Copyright (c) 2020-2024, NVIDIA CORPORATION.

cmake_minimum_required(VERSION 3.26.4)

project(
        basic_example
        VERSION 0.0.1
        LANGUAGES CXX CUDA
)
set(SM_DEF "-DSM700")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

include(set_cuda_architecture.cmake)

# initialize cuda architecture
rapids_cuda_init_architectures(basic_example)
rapids_cuda_set_architectures(RAPIDS)

include(fetch_dependencies.cmake)

# Add CUDA and C++ source files separately
set(CUDA_SOURCES
        src/volcano/join_exp.cu
)

set(CPP_SOURCES
        src/volcano/CudfJoin.cpp
)

# Create CUDA object library
add_library(cuda_objs OBJECT ${CUDA_SOURCES})
set_target_properties(cuda_objs PROPERTIES CUDA_SEPARABLE_COMPILATION ON)
target_compile_definitions(cuda_objs PRIVATE ${SM_DEF})

# Create C++ object library
add_library(cpp_objs OBJECT ${CPP_SOURCES})
target_link_libraries(cpp_objs PRIVATE cudf::cudf)
target_compile_definitions(cpp_objs PRIVATE ${SM_DEF})

# Create the main executable
add_executable(basic_example $<TARGET_OBJECTS:cuda_objs> $<TARGET_OBJECTS:cpp_objs>)

target_link_libraries(basic_example PRIVATE cudf::cudf)
target_compile_features(basic_example PRIVATE cxx_std_17)
target_compile_definitions(basic_example PRIVATE ${SM_DEF})

install(TARGETS basic_example DESTINATION bin/examples/libcudf)
install(FILES ${CMAKE_CURRENT_LIST_DIR}/4stock_5day.csv DESTINATION bin/examples/libcudf)