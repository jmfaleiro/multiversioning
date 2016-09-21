#ifndef 	RC_RECORD_H_
#define 	RC_RECORD_H_

#include <mcs.h>
#include <machine.h>

#define RC_RECORD_SIZE(value_sz)	(CACHE_LINE + sizeof(uint64_t) + value_sz)
#define RC_VALUE_PTR(record)		(&(((uint64_t*)&(((char*)record)[CACHE_LINE]))[1]))
#define RC_TIMESTAMP_PTR(record)	((uint64_t*)&(((char*)record)[CACHE_LINE]))
#define RC_LOCK_PTR(record)		((mcs_struct**)record)

#endif 		// RC_RECORD_H_
