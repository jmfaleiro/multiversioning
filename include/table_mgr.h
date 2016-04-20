#ifndef 		TABLE_MGR_H_
#define 		TABLE_MGR_H_

#include <table.h>
#include <concurrent_table.h>

class table_mgr {
 private:
        Table 			**_tables;
        concurrent_table 	**_conc_tables;
        uint32_t 		_ntables;

 public:
        table_mgr(Table **tables, concurrent_table **conc_tables, 
                  uint32_t ntables);
        concurrent_table* get_conc_table(uint32_t table_id);
        Table* get_table(uint32_t table_id);
        void set_init();
};

#endif 			// TABLE_MGR_H_
