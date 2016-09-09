#include <lock_manager.h>
#include <algorithm>

LockManager::LockManager(table_mgr *tbl_mgr)
{
        _tbl_mgr = tbl_mgr;
}

bool LockManager::LockRecord(struct locking_key *k)
{
        Table *tbl;
        void *record;
        mcs_rw::mcs_rw_lock *lock;

        assert(k->is_held == false);
        
        tbl = _tbl_mgr->get_table(k->table_id);
        assert(tbl != NULL);
        record = tbl->Get(k->key);
        assert(record != NULL);
        k->value = record;
        lock = (mcs_rw::mcs_rw_lock*)record;

        if (k->is_write) 
                acquire_writer(lock, &k->lock_node);
        else 
                acquire_reader(lock, &k->lock_node);
        return true;
}

void LockManager::UnlockRecord(struct locking_key *k)
{
        assert(k->is_held == true);
        
        mcs_rw::mcs_rw_lock *lock;
        
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
                        acquired = LockRecord(&txn->readset[*r_index]);
                        *r_index += 1;
                } else {
                        acquired = LockRecord(&txn->writeset[*w_index]);
                        *w_index += 1;
                }
        }
    
        /* At most one of these two loops can be executed. */
        while (acquired && *w_index < num_writes) {
                assert(*r_index == num_reads);
                acquired = LockRecord(&txn->writeset[*w_index]);
                *w_index += 1;
        }
        while (acquired && *r_index < num_reads) {
                assert(*w_index == num_writes);
                acquired = LockRecord(&txn->readset[*r_index]);
                *r_index += 1;
        }
        assert(acquired && *w_index == num_writes && *r_index == num_reads);
        return acquired;
}

#ifdef 	RUNTIME_PIPELINING

void LockManager::ReleaseTable(locking_action *txn, uint32_t table_id)
{
        uint32_t i, num_writes, num_reads;

        num_writes = txn->writeset.size();
        num_reads = txn->readset.size();
        for (i = txn->write_release; i < num_writes; ++i) {
                //                assert(txn->writeset[i].table_id >= table_id);
                if (txn->writeset[i].table_id <= table_id) {
                        UnlockRecord(&txn->writeset[i]);
                        txn->write_release += 1;
                } else {
                        break;
                }
        }
        
        for (i = txn->read_release; i < num_reads; ++i) {
                //                assert(txn->readset[i].table_id >= table_id);
                if (txn->readset[i].table_id <= table_id) {
                        UnlockRecord(&txn->readset[i]);
                        txn->read_release += 1;
                } else {
                        break;
                }
        }
}

void LockManager::Unlock(locking_action *txn)
{
        uint32_t i, num_writes, num_reads;

        num_writes = txn->writeset.size();
        num_reads = txn->readset.size();
        for (i = txn->write_release; i < num_writes; ++i) {
                UnlockRecord(&txn->writeset[i]);
        }
        for (i = txn->read_release; i < num_reads; ++i) {
                UnlockRecord(&txn->readset[i]);
        }
        txn->finished_execution = true;
}

#else 

void LockManager::Unlock(locking_action *txn)
{
        uint32_t i, num_writes, num_reads;

        num_writes = txn->writeset.size();
        num_reads = txn->readset.size();
        for (i = 0; i < num_writes; ++i) 
                UnlockRecord(&txn->writeset[i]);
        for (i = 0; i < num_reads; ++i) 
                UnlockRecord(&txn->readset[i]);
        txn->finished_execution = true;
}

#endif
