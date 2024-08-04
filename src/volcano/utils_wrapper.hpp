#ifndef UTILS_WRAPPER_H
#define UTILS_WRAPPER_H
#include "utils.cuh"
#include <iostream>
#include "mem_manager.hpp"
#include <cuda.h>
// Declare functions you want to use in .cpp files

// Forward declaration of the release_mem template function
template<typename T>
void release_mem(T* ptr, cudaStream_t stream = 0);

#endif