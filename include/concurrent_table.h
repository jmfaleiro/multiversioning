#ifndef 	CONCURRENT_TABLE_H_
#define 	CONCURRENT_TABLE_H_

#include <mcs.h>
#include <table.h>

struct conc_table_record {
        uint64_t key;
        uint64_t tid;
        void *value;
        struct conc_table_record *next;
};

struct concurrent_table_bckt {
        volatile mcs_struct __attribute__((__packed__, __aligned__(CACHE_LINE))) *_lock_tail;
        conc_table_record 		*_records;
}__attribute__((__packed__, __aligned__(CACHE_LINE)));

class concurrent_table {
 private:

        TableConfig 		conf;
        uint64_t 		_salt;
        uint64_t 		_tbl_sz;
        concurrent_table_bckt 	*_buckets;

        concurrent_table_bckt* get_bucket(uint64_t key)
        {
                uint64_t index;
                
                index = Hash128to64(std::make_pair(key, _salt)) % _tbl_sz;
                return &_buckets[index];
        }
        
 public:
        
        concurrent_table(uint64_t num_buckets)
        {
                _buckets = (concurrent_table_bckt*)zmalloc(sizeof(concurrent_table_bckt)*num_buckets);
                _salt = (uint64_t)rand();
                _tbl_sz = num_buckets;
        }

        virtual void* LockedGet(uint64_t key, mcs_struct *lock_struct) 
        {
                concurrent_table_bckt *bucket;
                conc_table_record *iter;
                void *ret;
                
                ret = NULL;
                bucket = get_bucket(key);
                lock_struct->_tail_ptr = &bucket->_lock_tail;
                mcs_mgr::lock(lock_struct);
                iter = bucket->_records;
                while (iter != NULL) {
                        if (iter->key == key) {
                                ret = iter->value;
                                break;
                        }
                        iter = iter->next;
                }
                if (iter == NULL) 
                        mcs_mgr::unlock(lock_struct);
                return ret;
        }
        
        virtual void* Get(uint64_t key, mcs_struct *lock_struct) 
        {
                concurrent_table_bckt *bucket;
                conc_table_record *iter;
                void *ret;
                
                ret = NULL;
                bucket = get_bucket(key);
                lock_struct->_tail_ptr = &bucket->_lock_tail;
                mcs_mgr::lock(lock_struct);
                iter = bucket->_records;
                while (iter != NULL) {
                        if (iter->key == key) {
                                ret = iter->value;
                                break;
                        }
                        iter = iter->next;
                }
                mcs_mgr::unlock(lock_struct);
                return ret;
        }

        virtual bool Put(conc_table_record *value, mcs_struct *lock_struct)
        {
                bool success;
                concurrent_table_bckt *bucket;
                conc_table_record *iter;

                bucket = get_bucket(value->key);
                success = true;
                lock_struct->_tail_ptr = &bucket->_lock_tail;
                mcs_mgr::lock(lock_struct);
                iter = bucket->_records;
                
                /* Search for a duplicate */
                while (iter != NULL) {
                        if (iter->key == value->key) {
                                success = false;
                                break;
                        }                                
                        iter = iter->next;
                }
                
                /* No duplicate, insert record */
                if (success == true) {
                        value->next = bucket->_records;
                        bucket->_records = value;
                }
                mcs_mgr::unlock(lock_struct);
                return success;
        }
        
        virtual void Remove(conc_table_record *rec, mcs_struct *lock_struct) 
        {
                concurrent_table_bckt *bucket;
                conc_table_record *iter, *prev;
                uint64_t key;
                assert(rec != NULL);

                key = rec->key;
                bucket = get_bucket(key);
                lock_struct->_tail_ptr = &bucket->_lock_tail;
                mcs_mgr::lock(lock_struct);
                
                prev = NULL;
                iter = bucket->_records;
                while (iter != NULL) {
                        if (iter == rec) 
                                break;
                        prev = iter;
                        iter = iter->next;
                }

                /* Shouldn't be asked to remove a non-existent record */
                assert(iter == rec);
                
                /* Remove it */
                if (prev == NULL) {
                        assert(bucket->_records == iter);
                        bucket->_records = iter->next;
                } else {
                        assert(bucket->_records != iter);
                        prev->next = iter->next;
                }

                mcs_mgr::unlock(lock_struct);
                iter->next = NULL;
        }
};

#endif 		// CONCURRENT_TABLE_H_
