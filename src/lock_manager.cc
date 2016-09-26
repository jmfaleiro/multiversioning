#include <lock_manager.h>
#include <algorithm>
#include <locking_record.h>
#include <tpcc.h>
#include <eager_worker.h>
#include <pipelined_record.h>

extern uint32_t cc_type;

LockManager::LockManager(table_mgr *tbl_mgr)
{
        _tbl_mgr = tbl_mgr;
}

void LockManager::commit_write(locking_action *txn, struct locking_key *k)
{
        uint32_t record_sz;
        
        record_sz = txn->bufs->GetRecordSize(k->table_id);
        if (cc_type == 1) {
                memcpy(LOCKING_VALUE_PTR(k->value), k->buf, record_sz);
        } else {
                assert(cc_type == 5);
                memcpy(PIPELINED_VALUE_PTR(k->value), k->buf, record_sz);
        }
        txn->bufs->ReturnRecord(k->table_id, k->buf);
        k->buf = NULL;
}

bool LockManager::LockRecord(locking_action *txn, struct locking_key *k)
{
        Table *tbl;
        void *record;
        mcs_rw::mcs_rw_lock *lock;

        assert(k->is_held == false);
        
        if (TPCC) {

                uint64_t wh_id, d_id;
        
                if (k->table_id == WAREHOUSE_TABLE) {
                        wh_id = k->key;
                        record = tpcc_config::warehouses[wh_id];
                } else if (k->table_id == DISTRICT_TABLE) {
                        wh_id = tpcc_util::get_warehouse_key(k->key);
                        d_id = tpcc_util::get_district_key(k->key);
                        record = tpcc_config::districts[wh_id][d_id];
                } else {
                        tbl = _tbl_mgr->get_table(k->table_id);
                        assert(tbl != NULL);
                        if (k->is_write)
                                record = tbl->GetAlways(k->key);
                        else
                                record = tbl->Get(k->key);
                        assert(record != NULL);
                }
        } else {


                tbl = _tbl_mgr->get_table(k->table_id);
                assert(tbl != NULL);
                if (k->is_write)
                        record = tbl->GetAlways(k->key);
                else
                        record = tbl->Get(k->key);
                assert(record != NULL);
        }


        
        k->value = record;
        k->buf = NULL;
        lock = (mcs_rw::mcs_rw_lock*)record;

        if (k->is_write) 
                acquire_writer(lock, &k->lock_node);
        else 
                acquire_reader(lock, &k->lock_node);
        k->is_held = true;
        assert(k->value != NULL);
        return true;
}

void LockManager::UnlockRecord(locking_action *txn, struct locking_key *k)
{
        assert(k->is_held == true && k->value != NULL);
        
        mcs_rw::mcs_rw_lock *lock;
        
        if (k->buf != NULL)
                commit_write(txn, k);
        assert(k->buf == NULL);
        lock = (mcs_rw::mcs_rw_lock*)k->value;
        if (k->is_write)
                release_writer(lock, &k->lock_node);
        else
                release_reader(lock, &k->lock_node);
}

bool LockManager::SortCmp(const locking_key &key1, const locking_key &key2)
{
        return key1 < key2;
}

bool LockManager::Lock(locking_action *txn)
{
        uint32_t *r_index, *w_index, num_reads, num_writes;
        struct locking_key write_key, read_key;
        bool acquired = true;

        txn->prepare();
        
        r_index = &txn->read_index;
        w_index = &txn->write_index;
        num_reads = txn->readset.size();
        num_writes = txn->writeset.size();

        /* Acquire locks in sorted order. */
        while (acquired && *r_index < num_reads && *w_index < num_writes) {
                read_key = txn->readset[*r_index];
                write_key = txn->writeset[*w_index];
                
                /* 
                 * A particular key can occur in one of the read- or write-sets,
                 * NOT both!  
                 */
                assert(read_key != write_key);                
                if (read_key < write_key) {
                        acquired = LockRecord(txn, &txn->readset[*r_index]);
                        *r_index += 1;
                } else {
                        acquired = LockRecord(txn, &txn->writeset[*w_index]);
                        *w_index += 1;
                }
        }
    
        /* At most one of these two loops can be executed. */
        while (acquired && *w_index < num_writes) {
                assert(*r_index == num_reads);
                acquired = LockRecord(txn, &txn->writeset[*w_index]);
                *w_index += 1;
        }
        while (acquired && *r_index < num_reads) {
                assert(*w_index == num_writes);
                acquired = LockRecord(txn, &txn->readset[*r_index]);
                *r_index += 1;
        }
        assert(acquired && *w_index == num_writes && *r_index == num_reads);
        return acquired;
}

void LockManager::Unlock(locking_action *txn)
{
        uint32_t i, num_writes, num_reads;

        num_writes = txn->writeset.size();
        num_reads = txn->readset.size();
        for (i = 0; i < num_writes; ++i) 
                UnlockRecord(txn, &txn->writeset[i]);
        for (i = 0; i < num_reads; ++i) 
                UnlockRecord(txn, &txn->readset[i]);
        txn->finished_execution = true;
}

