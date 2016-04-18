#ifndef 	CONCURRENT_TABLE_H_
#define 	CONCURRENT_TABLE_H_

#include <mcs.h>
#include <table.h>

/*
struct conc_table_record {
        uint64_t key;
        uint64_t tid;
        void *value;
        struct conc_table_record *next;
};
*/

struct concurrent_table_bckt {
        volatile mcs_struct __attribute__((__packed__, __aligned__(CACHE_LINE))) *_lock_tail;
        TableRecord 		*_records;
}__attribute__((__packed__, __aligned__(CACHE_LINE)));

class concurrent_table {
 private:

        TableConfig 		conf;
        uint64_t 		_salt;
        uint64_t 		_tbl_sz;
        concurrent_table_bckt 		*_buckets;

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
                TableRecord *iter;
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
                TableRecord *iter;
                void *ret;
                
                ret = NULL;
                bucket = get_bucket(key);
                lock_struct->_tail_ptr = &bucket->_lock_tail;
                mcs_mgr::lock(lock_struct);
                iter = bucket->_records;
                while (iter != NULL && iter->key > key) 
                        iter = iter->next;

                if (iter != NULL && iter->key == key)                         
                        ret = iter->value;

                mcs_mgr::unlock(lock_struct);                
                return ret;
        }

        virtual bool Put(TableRecord *value, mcs_struct *lock_struct)
        {
                //                bool success;
                concurrent_table_bckt *bucket;
                //                conc_table_record *iter, **prev;

                bucket = get_bucket(value->key);
                //                success = false;
                lock_struct->_tail_ptr = &bucket->_lock_tail;
                mcs_mgr::lock(lock_struct);
                
                /* Search for a duplicate */
                value->next = bucket->_records;
                bucket->_records = value;
                /*
                prev = &bucket->_records;
                iter = bucket->_records;
                while (iter != NULL && iter->key > value->key) {
                        prev = &iter->next;
                        iter = iter->next;                        
                }
                
                if (iter == NULL || iter->key < value->key) {
                        success = true;
                        value->next = iter;
                        *prev = value;
                }
                */
                mcs_mgr::unlock(lock_struct);
                return true;
        }
        
        virtual void Remove(TableRecord *rec, mcs_struct *lock_struct) 
        {
                concurrent_table_bckt *bucket;
                TableRecord *iter, **prev;
                uint64_t key;
                assert(rec != NULL);

                key = rec->key;
                bucket = get_bucket(key);
                lock_struct->_tail_ptr = &bucket->_lock_tail;
                mcs_mgr::lock(lock_struct);
                
                prev = &bucket->_records;
                iter = bucket->_records;
                while (iter != NULL && iter != rec) {
                        prev = &iter->next;
                        iter = iter->next;
                }

                /* Shouldn't be asked to remove a non-existent record */
                assert(iter == rec);
                
                /* Remove it */
                *prev = iter->next;

                mcs_mgr::unlock(lock_struct);
                iter->next = NULL;
        }
};

#endif 		// CONCURRENT_TABLE_H_
