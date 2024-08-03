// In join_kernels.cu
#include "join_kernels.cuh"

template<tyname T>
void sort_on_gpu(T* keys, int num_items){
    T* d_keys;
    T* d_sorted_keys;
    cudaMalloc(&d_keys, sizeof(T)*num_items);
    cudaMemcpy(d_keys, keys, sizeof(T)*num_items, cudaMemcpyDefault);
    cudaMalloc(&d_sorted_keys, sizeof(T)*num_items);

    void* d_temp_storage = nullptr;
    size_t temp_storage_bytes = 0;
    cub::DeviceRadixSort::SortKeys(nullptr, temp_storage_bytes, d_keys, d_sorted_keys, num_items);
    cudaMalloc(&d_temp_storage, temp_storage_bytes);
    cub::DeviceRadixSort::SortKeys(d_temp_storage, temp_storage_bytes, d_keys, d_sorted_keys, num_items);

    cudaMemcpy(keys, d_sorted_keys, sizeof(T)*num_items, cudaMemcpyDefault);
    cudaFree(d_keys);
    cudaFree(d_sorted_keys);
    cudaDeviceSynchronize();
}

// Explicit instantiation for types you'll use
template void sort_on_gpu<int>(int* keys, int num_items);
template void sort_on_gpu<long>(long *keys, int num_items);
// Add other types as needed.