#include <table_mgr.h>

table_mgr::table_mgr(Table **tables, concurrent_table **conc_tables, 
                     uint32_t ntables)
{
        _tables = tables;
        _conc_tables = conc_tables;
        _ntables = ntables;
}

Table* table_mgr::get_table(uint32_t table_id) 
{
        assert(_tables[table_id] != NULL);
        return _tables[table_id];
}

concurrent_table* table_mgr::get_conc_table(uint32_t table_id)         
{
        assert(_conc_tables[table_id] != NULL);
        return _conc_tables[table_id];
}

void table_mgr::set_init()
{
        uint32_t i;
        for (i = 0; i < _ntables; ++i) {
                if (_tables[i] != NULL) 
                        _tables[i]->SetInit();
        }
}
