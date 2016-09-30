#ifndef 	INSERT_BUF_MGR_H_
#define 	INSERT_BUF_MGR_H_

#include <concurrent_table.h>

#define 	ALLOC_INCREMENTS 	(((uint64_t)1)<<25)

/* Hard code this shit */
class insert_buf_mgr {
 private:
        int 			_cpu;
        uint32_t 		_ntables;
        size_t	 		*_record_sizes;
        conc_table_record	**_conc_allocs;
        TableRecord		**_tbl_allocs;

        void alloc_single(uint32_t table_id);
        void alloc_entries(uint32_t table_id, size_t record_sz, uint64_t alloc_sz);

 public:
        void* operator new(std::size_t sz, int cpu);
        insert_buf_mgr(int cpu, uint32_t ntables, size_t *record_sizes, 
                       bool conc_records);
        conc_table_record* get_insert_record(uint32_t table_id);
        TableRecord* get_table_record(uint32_t table_id);
        void return_table_record(TableRecord *record, uint32_t table_id);
        void return_insert_record(conc_table_record *record, uint32_t table_id);
};

#endif 		// INSERT_BUF_MGR_H_
