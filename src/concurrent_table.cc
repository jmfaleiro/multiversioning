#include <concurrent_table.h>
#include <tpcc.h>

/* Get bucket corresponding to a particular key */
concurrent_table_bckt* concurrent_table::get_bucket(uint64_t key) 
{
        uint64_t index;
        uint32_t wh, d;
                
        wh = tpcc_util::get_warehouse_key(key);
        d = tpcc_util::get_district_key(key);
        assert(wh < tpcc_config::num_warehouses);
        assert(d < NUM_DISTRICTS);
        index = Hash128to64(std::make_pair(key, _salt)) % _tbl_sz;
        return &_buckets[wh][d][index];
}

concurrent_table::concurrent_table(uint64_t num_buckets)
{
        uint32_t i, j;
        uint64_t bucket_sub;
        
        /* 
         * Partition a table by warehouse+district ids. Makes it more efficient 
         * than a non-partitioned table. Allows transactions that probe the 
         * table to expect an ordering among records within a single bucket. 
         * XXX Ideally, just create an ordered index.
         */
        bucket_sub = num_buckets / (tpcc_config::num_warehouses * NUM_DISTRICTS);
        _buckets = (concurrent_table_bckt***)zmalloc(sizeof(concurrent_table_bckt**)*tpcc_config::num_warehouses);
        for (i = 0; i < tpcc_config::num_warehouses; ++i) {
                _buckets[i] = (concurrent_table_bckt**)zmalloc(sizeof(concurrent_table_bckt*)*NUM_DISTRICTS);
                for (j = 0; j < NUM_DISTRICTS; ++j) {
                        _buckets[i][j] = (concurrent_table_bckt*)zmalloc(sizeof(concurrent_table_bckt)*bucket_sub);
                }
        }                

        _salt = (uint64_t)rand();
        _tbl_sz = bucket_sub;
}

/* Return a reference to an inserted record */
bool concurrent_table::Get(uint64_t key, mcs_struct *lock_struct, 
                           conc_table_record **value)
{
        concurrent_table_bckt *bucket;
        conc_table_record *iter;
        bool success;
                
        success = false;
        bucket = get_bucket(key);
        lock_struct->_tail_ptr = &bucket->_lock_tail;
        mcs_mgr::lock(lock_struct);

        iter = bucket->_records;
        while (iter != NULL && iter->key > key) 
                iter = iter->next;
        
        if (iter != NULL && iter->key == key) {
                *value = iter;
                success = true;
        }

        mcs_mgr::unlock(lock_struct);                
        return success;
}

/* Attempt to remove a record. Only succeeds if record's ref_count is 1. */
bool concurrent_table::RemoveRecord(conc_table_record *record, 
                                    mcs_struct *lock_struct)
{
        concurrent_table_bckt *bucket;
        conc_table_record *iter, **prev;
        bool success;

        //        if ((temp = fetch_and_decrement(&record->ref_count)) > 0)
        //                return false;
                
        success = false;
        bucket = get_bucket(record->key);
        lock_struct->_tail_ptr = &bucket->_lock_tail;
                
        mcs_mgr::lock(lock_struct);
                
        prev = &bucket->_records;
        iter = bucket->_records;
        while (iter != NULL && iter->key > record->key) {
                prev = &iter->next;
                iter = iter->next;
        }

        assert(iter == record);
        if (fetch_and_decrement(&iter->ref_count) == 0) {
                *prev = iter->next;                     
                iter->next = NULL;
                success = true;
        } else {
                success = false;
        }
                
        mcs_mgr::unlock(lock_struct);
        return success;
}

bool concurrent_table::Put(conc_table_record *record_ptr, mcs_struct *lock_struct)
{
        assert(record_ptr->ref_count == 0);
        bool success;
        concurrent_table_bckt *bucket;
        conc_table_record *iter, **prev;
        
        record_ptr->next = NULL;
        success = false;
        bucket = get_bucket(record_ptr->key);
        lock_struct->_tail_ptr = &bucket->_lock_tail;
                
        mcs_mgr::lock(lock_struct);
                
        prev = &bucket->_records;
        iter = bucket->_records;
        while (iter != NULL && iter->key > record_ptr->key) {
                prev = &iter->next;
                iter = iter->next;                        
        }
                
        if (iter == NULL || iter->key < record_ptr->key) {
                record_ptr->next = iter;
                *prev = record_ptr;
                success = true;
                record_ptr->ref_count = 1;
        } else {
                assert(iter != NULL && iter->key == record_ptr->key);
                assert(iter != record_ptr);
                success = false;
                fetch_and_increment(&iter->ref_count);
        }                

        mcs_mgr::unlock(lock_struct);
        return success;
}
