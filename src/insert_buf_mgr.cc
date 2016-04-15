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
        conc_table_record *header_data;

        num_records = alloc_sz / record_sz;
        
        /* Allocate data */
        header_data = (conc_table_record*)zmalloc(sizeof(conc_table_record)*num_records);
        data = (char*)zmalloc(record_sz*num_records);

        /* Setup */
        for (i = 0; i < num_records; ++i) {                
                header_data[i].value = &data[i*record_sz];
                header_data[i].next = &header_data[i+1];
        }
        
        /* Add to allocation queue */
        header_data[i-1].next = _conc_allocs[table_id];
        _conc_allocs[table_id] = header_data;
}

void insert_buf_mgr::alloc_single(uint32_t table_id)
{
        assert(table_id == NEW_ORDER_TABLE ||
               table_id == OORDER_TABLE ||
               table_id == ORDER_LINE_TABLE ||
               table_id == HISTORY_TABLE);
        alloc_entries(table_id, _record_sizes[table_id], ALLOC_INCREMENTS);
}

insert_buf_mgr::insert_buf_mgr(int cpu, uint32_t ntables, size_t *record_sz)
{
        _cpu = cpu;
        _ntables = ntables;
        _record_sizes = (size_t*)alloc_mem(sizeof(size_t)*ntables, cpu);
        assert(_record_sizes != NULL);
        memcpy(_record_sizes, record_sz, sizeof(size_t)*ntables);
        for (uint32_t i = 0; i < ntables; ++i)
                _record_sizes[i] += 8;
        _conc_allocs = (conc_table_record**)alloc_mem(sizeof(conc_table_record*)*ntables, cpu);
        assert(_conc_allocs != NULL);
        memset(_conc_allocs, 0x0, sizeof(conc_table_record*)*ntables);
}

conc_table_record* insert_buf_mgr::get_insert_record(uint32_t table_id) 
{
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
        record->next = _conc_allocs[table_id];
        _conc_allocs[table_id] = record;        
}
