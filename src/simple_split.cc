#include <simple_split.h>

simple_split::simple_split(vector<uint64_t> records)
{
        uint32_t i, sz;
        
        sz = records.size();
        this->records = new vector<uint64_t>();
        for (i = 0; i < sz; ++i) 
                this->records->push_back(records[i]);
}

/* 
 * Each db record is a uint64_t. A simple_split transaction increments each such
 * record by 1.
 */
bool simple_split::Run()
{
        uint32_t i;
        uint64_t *val;

        for (i = 0; i < records->size(); ++i) {
                val = (uint64_t*)get_write_ref((*records)[i], 0);
                *val += 1;                
        }
}

/* Txn interface functions */
uint32_t simple_split::num_rmws()
{
        return records->size();
}

void simple_split::get_rmws(struct big_key *array)
{
        uint32_t i, record_count;
        
        record_count = records->size();
        for (i = 0; i < record_count; ++i) {
                array[i].key = (*records)[i];
                array[i].table_id = 0;
        }
}
