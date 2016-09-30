#ifndef 	CONCURRENT_TABLE_H_
#define 	CONCURRENT_TABLE_H_

#include <mcs.h>
#include <table.h>
#include <util.h>

struct conc_table_record {
        struct conc_table_record *next;
        struct conc_table_record *next_in;
        uint64_t key;
        uint32_t table_id;
        uint32_t state;
        volatile uint64_t ref_count;
        char *value;
        struct conc_table_record *inserted_record;
};

struct concurrent_table_bckt {
        volatile mcs_struct __attribute__((__packed__, __aligned__(CACHE_LINE))) *_lock_tail;
        conc_table_record 		*_records;
}__attribute__((__packed__, __aligned__(CACHE_LINE)));

class concurrent_table {
 protected:

        TableConfig 			conf;
        uint64_t 			_salt;
        uint64_t 			_tbl_sz;
        concurrent_table_bckt 		***_buckets;

        virtual concurrent_table_bckt* get_bucket(uint64_t key);
        
 public:
        
        concurrent_table(uint64_t num_buckets);
        virtual bool Get(uint64_t key, mcs_struct *lock_struct, 
                         conc_table_record **value);
        virtual bool RemoveRecord(conc_table_record *record, mcs_struct *lock_struct);
        virtual bool Put(conc_table_record *value, mcs_struct *lock_struct);
};

#endif 		// CONCURRENT_TABLE_H_
