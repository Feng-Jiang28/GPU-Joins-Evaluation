// In join_kernels.cuh
#ifnef JOIN_KERNELS.CUH
#define JOIN_KERNELS_CUH

template<typename T>
void sort_on_gpu(T* keys, int num_items);

#endif // JOIN_KERNELS_CUH