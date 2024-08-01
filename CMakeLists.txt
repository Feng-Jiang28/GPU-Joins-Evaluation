cmake_minimum_required(VERSION 3.26.4)

project(
        crystal
        VERSION 0.0.1
        LANGUAGES CXX CUDA
)

# Set CUDA paths
set(CUDA_PATH "/usr/local/cuda" CACHE PATH "Path to CUDA installation")
set(CUDA_INC_PATH "${CUDA_PATH}/include")
set(CUDA_BIN_PATH "${CUDA_PATH}/bin")

# Set CUDA architecture
set(SM_TARGETS "gencode=arch=compute_70,code=\"sm_70,compute_70\"")
set(SM_DEF "-DSM700")

list(APPEND CUDA_NVCC_FLAGS ${SM_DEF})
# Uncomment the following lines for A100
# set(SM_TARGETS "-gencode=arch=compute_80,code=\"sm_80,compute_80\"")
# set(SM_DEF "-DSM800")

# Set CUDA flags
set(CUDA_NVCC_FLAGS ${CUDA_NVCC_FLAGS}
        --std=c++17
        --expt-relaxed-constexpr
        --expt-extended-lambda
        --extended-lambda
        ${SM_DEF}
        -Xptxas="-v"
        -lineinfo
        -Xcudafe -\#
)

# Include directories
include_directories(${CUDA_INC_PATH} cub src)

# Find all CUDA source files
file(GLOB_RECURSE CU_SOURCES "src/*.cu")

# Create executable targets for each CUDA source file
foreach(CU_SOURCE ${CU_SOURCES})
    get_filename_component(CU_NAME ${CU_SOURCE} NAME_WE)
    add_executable(${CU_NAME} ${CU_SOURCE})
    set_target_properties(${CU_NAME} PROPERTIES CUDA_SEPARABLE_COMPILATION ON)
    target_compile_definitions(${CU_NAME} PRIVATE ${SM_DEF})  # Add this line
    target_link_libraries(${CU_NAME} PRIVATE gomp)
    install(TARGETS ${CU_NAME} DESTINATION bin)
endforeach()

# Custom targets
add_custom_target(setup
        COMMAND ${CMAKE_COMMAND} -E make_directory bin/volcano obj/volcano
)

add_custom_target(clean_all
        COMMAND ${CMAKE_COMMAND} -E remove_directory bin
        COMMAND ${CMAKE_COMMAND} -E remove_directory obj
)