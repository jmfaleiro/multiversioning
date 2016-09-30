#include <insert_buf_mgr.h>
#include <tpcc.h>

void* insert_buf_mgr::operator new(std::size_t sz, int cpu)
{
        return alloc_mem(sz, cpu);
}

void insert_buf_mgr::alloc_entries(uint32_t table_id, size_t record_sz, 
                                   uint64_t alloc_sz)
{
        assert(table_id == NEW_ORDER_TABLE ||
               table_id == OORDER_TABLE ||
               table_id == ORDER_LINE_TABLE ||
               table_id == HISTORY_TABLE);
        
        uint64_t i, num_records;
        char *data;
        conc_table_record *c_headers;
        TableRecord *t_headers;        
        
        /* Allocate data */
        num_records = alloc_sz / record_sz;
        data = (char*)zmalloc(record_sz*num_records);

        if (_conc_allocs != NULL) {
                assert(_tbl_allocs == NULL);
                c_headers = (conc_table_record*)zmalloc(sizeof(conc_table_record)*num_records);

                /* Setup */
                for (i = 0; i < num_records; ++i) {                
                        c_headers[i].next = &c_headers[i+1];
                        c_headers[i].value = (data + record_sz*i);
                }
                c_headers[i-1].next = NULL;
        
                /* Add to allocation queue */
                _conc_allocs[table_id] = c_headers;

        } else {
                assert(_tbl_allocs != NULL);
                t_headers = (TableRecord*)zmalloc(sizeof(TableRecord)*num_records);

                /* Setup */
                for (i = 0; i < num_records; ++i) {                
                        t_headers[i].next = &t_headers[i+1];
                        t_headers[i].value = (data + record_sz*i);
                }
                t_headers[i-1].next = NULL;
        
                /* Add to allocation queue */
                _tbl_allocs[table_id] = t_headers;
        }
}

void insert_buf_mgr::alloc_single(uint32_t table_id)
{
        assert(table_id == NEW_ORDER_TABLE ||
               table_id == OORDER_TABLE ||
               table_id == ORDER_LINE_TABLE ||
               table_id == HISTORY_TABLE);
        alloc_entries(table_id, _record_sizes[table_id], ALLOC_INCREMENTS);
}

insert_buf_mgr::insert_buf_mgr(int cpu, uint32_t ntables, size_t *record_sz, 
                               bool conc_records)
{
        _cpu = cpu;
        _ntables = ntables;
        _record_sizes = (size_t*)alloc_mem(sizeof(size_t)*ntables, cpu);
        assert(_record_sizes != NULL);
        memcpy(_record_sizes, record_sz, sizeof(size_t)*ntables);
        for (uint32_t i = 0; i < ntables; ++i)
                _record_sizes[i] += 8;
        
        if (conc_records == true) {
                _tbl_allocs = NULL;
                _conc_allocs = (conc_table_record**)alloc_mem(sizeof(conc_table_record*)*ntables, cpu);
                assert(_conc_allocs != NULL);
                memset(_conc_allocs, 0x0, sizeof(conc_table_record*)*ntables);
        } else {
                _conc_allocs = NULL;
                _tbl_allocs = (TableRecord**)alloc_mem(sizeof(TableRecord*)*ntables, cpu);
                assert(_tbl_allocs != NULL);
                memset(_tbl_allocs, 0x0, sizeof(TableRecord*)*ntables);
        }
}

conc_table_record* insert_buf_mgr::get_insert_record(uint32_t table_id) 
{
        assert(_conc_allocs != NULL && _tbl_allocs == NULL);
        conc_table_record *ret;

        if (_conc_allocs[table_id] == NULL) 
                alloc_single(table_id);

        assert(_conc_allocs[table_id] != NULL);
        ret = _conc_allocs[table_id];
        _conc_allocs[table_id] = ret->next;
        ret->next = NULL;
        return ret;        
}

void insert_buf_mgr::return_insert_record(conc_table_record *record, 
                                          uint32_t table_id)
{
        assert(record->ref_count == 0);
        assert(_conc_allocs != NULL && _tbl_allocs == NULL);
        record->next = _conc_allocs[table_id];
        _conc_allocs[table_id] = record;        
}

TableRecord* insert_buf_mgr::get_table_record(uint32_t table_id) 
{
        assert(_conc_allocs == NULL && _tbl_allocs != NULL);
        TableRecord *ret;

        if (_tbl_allocs[table_id] == NULL) 
                alloc_single(table_id);

        assert(_tbl_allocs[table_id] != NULL);
        ret = _tbl_allocs[table_id];
        _tbl_allocs[table_id] = ret->next;
        ret->next = NULL;
        return ret;
}

void insert_buf_mgr::return_table_record(TableRecord *record, 
                                         uint32_t table_id)
{
        assert(_conc_allocs == NULL && _tbl_allocs != NULL);
        record->next = _tbl_allocs[table_id];
        _tbl_allocs[table_id] = record;        
}
