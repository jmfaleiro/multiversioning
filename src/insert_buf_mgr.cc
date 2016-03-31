#include <insert_buf_mgr.h>
#include <tpcc.h>

void insert_buf_mgr::alloc_entries(uint32_t table_id, size_t record_sz, 
                                   uint64_t alloc_sz)
{
        assert(table_id == NEW_ORDER_TABLE ||
               table_id == OORDER_TABLE ||
               table_id == ORDER_LINE_TABLE ||
               table_id == HISTORY_TABLE);
        
        uint64_t i, num_records;
        
        num_records = alloc_sz / record_sz;
        
        /* Allocate data */
        header_data = zmalloc(sizeof(conc_table_record)*num_records);
        data = (char*)zmalloc(record_sz*num_records);

        /* Setup */
        for (i = 0; i < num_records; ++i) {                
                header_data[i].value = data[i];
                header_data[i].next = &header_data[i+1];
        }
        
        /* Add to allocation queue */
        header_data[i-1].next = _conc_allocs[table_id];
        _conc_allocs[table_id] = header_data;
}

void insert_buf_mgr::init_single(uint32_t table_id)
{
        assert(table_id == NEW_ORDER_TABLE ||
               table_id == OORDER_TABLE ||
               table_id == ORDER_LINE_TABLE ||
               table_id == HISTORY_TABLE);
        
        double total, fraction;
        uint64_t total_size, num_records, record_size, i;
        char *header_data, *data;

        total = sizeof(new_order_record)*1.0 + 
                sizeof(oorder_record)*1.0 + 
                sizeof(order_line_record)*12.0 +
                sizeof(history_record)*1.0;
        
        record_size = _record_sizes[table_id];
        fraction = (1.0*_record_sizes[table_id]) / total;

        if (table_id == NEW_ORDER_TABLE) {
                record_size = sizeof(new_order_record);
                fraction = (1.0*record_size) / total;
        } else if (table_id == OORDER_TABLE) {
                record_size = sizeof(oorder_record);
                fraction = (1.0*sizeof(oorder_record)) / total;
        } else if (table_id == ORDER_LINE_TABLE) {
                record_size = sizeof(order_line_record);
                fraction = (12.0*sizeof(order_line_record))/ total;
        } else if (table_id == HISTORY_TABLE) {
                record_size = sizeof(history_record);
                fraction = (1.0*sizeof(history_record)) / total;
        } else {
                assert(false);
        }
        
        size = (uint64_t)(fraction * ((double)_sz));
        num_records = size/record_size;
        alloc_entries(table_id, record_size, size);
}

void insert_buf_mgr::alloc_entries(uint32_t table_id, size_t record_sz, 
                                   uint64_t alloc_sz);


insert_buf_mgr::alloc_entries(uint32_t table_id)
{
        alloc_entries(table_id, _record_sizes[table_id], ALLOC_INCREMENTS);
}

insert_buf_mgr::insert_buf_mgr(uint64_t sz)
{
        _sz = sz;
        
}

