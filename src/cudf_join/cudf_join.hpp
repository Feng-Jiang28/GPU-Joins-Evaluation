#pragma once
#include <iostream>

void prepare_running(int argc, char** argv);

template<typename TupleR, typename TupleS, typename TupleOut>
void free_tuple_mem(TupleR r, TupleS s, TupleOut out);

template<typename join_key_t, typename col_t, typename TupleR, typename TupleS>
void prepare_workload(const struct join_args& args, TupleR& relation_r, TupleS& relation_s);

inline std::string get_utc_time();