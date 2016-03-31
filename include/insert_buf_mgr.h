#ifndef 	INSERT_BUF_MGR_H_
#define 	INSERT_BUF_MGR_H_

#include <concurrent_table.h>

#define 	ALLOC_INCREMENTS 	(((uint64_t)1)<<25)

/* Hard code this shit */
class insert_buf_mgr {
 private:
        uint32_t 		_ntables;
        size_t	 		*_record_sizes;
        conc_table_record 	**_conc_allocs;

        void init_single(uint32_t table_id);

 public:
        insert_buf_mgr();
        conc_table_record* get_insert_record(uint32_t table_id);
        void return_insert_record(conc_table_record *record, uint32_t table_id);
};

#endif 		// INSERT_BUF_MGR_H_
