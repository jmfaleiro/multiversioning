#ifndef 	LOCKING_RECORD_H_
#define 	LOCKING_RECORD_H_

#include <mcs_rw.h>

#define LOCKING_RECORD_SIZE(value_sz) (sizeof(mcs_rw::mcs_rw_lock) + value_sz)
#define LOCKING_VALUE_PTR(record) &(((mcs_rw::mcs_rw_lock*)record)[1])

#endif 		// LOCKING_RECORD_H_
