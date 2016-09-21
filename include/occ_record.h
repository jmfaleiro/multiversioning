#ifndef 	OCC_RECORD_H_
#define 	OCC_RECORD_H_

#define OCC_RECORD_SIZE(value_sz)	(sizeof(uint64_t) + value_sz)
#define OCC_VALUE_PTR(record)		((void*)(&(((uint64_t*)record)[1])))
#define OCC_TIMESTAMP_PTR(record)	((uint64_t*)record)

#endif 		// OCC_RECORD_H_
