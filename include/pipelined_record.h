#ifndef 	PIPELINED_RECORD_H_
#define 	PIPELINED_RECORD_H_

#include <mcs_rw.h>

#define PIPELINED_RECORD_SIZE(value_sz) 	(sizeof(mcs_rw::mcs_rw_lock) + sizeof(uint64_t) + value_sz)
#define PIPELINED_VALUE_PTR(record) 		(&((uint64_t*)(&(((mcs_rw::mcs_rw_lock*)record)[1])))[1])
#define PIPELINED_DEP_PTR(record) 		(&(((mcs_rw::mcs_rw_lock*)record)[1]))

#endif 		// PIPELINED_RECORD_H_
