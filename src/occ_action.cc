#include <occ_action.h>
#include <algorithm>
#include <occ.h>
#include <tpcc.h>
#include <occ_record.h>
#include <rc_record.h>

extern uint32_t READ_COMMITTED;

bool OCCAction::try_acquire_single(volatile uint64_t *lock_ptr)
{
        volatile uint64_t cmp_tid, locked_tid;
        barrier();
        cmp_tid = *lock_ptr;
        barrier();
        if (IS_LOCKED(cmp_tid))
                return false;        
        locked_tid = (cmp_tid | cpu_id);
        return cmp_and_swap(lock_ptr, cmp_tid, locked_tid);
}

void OCCAction::acquire_single(volatile uint64_t *lock_ptr)
{
        uint32_t backoff, temp;
         if (USE_BACKOFF) 
                 backoff = 1;
        while (true) {
                if (try_acquire_single(lock_ptr)) {
                        assert(IS_LOCKED(*lock_ptr));
                        break;
                }
                if (USE_BACKOFF) {
                         temp = backoff;
                         while (temp-- > 0)
                                 single_work();
                         backoff = backoff*2;
                }
        }
}

void OCCAction::release_single(volatile uint64_t *lock_word)
{
        uint64_t old_tid, xchged_tid;

        barrier();
        old_tid = *lock_word;
        barrier();

        old_tid = xchgq(lock_word, GET_TIMESTAMP(old_tid));
        assert(IS_LOCKED(old_tid));
        assert(GET_LOCK_HOLDER(old_tid) == cpu_id);
}

occ_composite_key::occ_composite_key(uint32_t table_id, uint64_t key,
                                     bool is_rmw)
{
        this->tableId = table_id;
        this->key = key;
        this->is_rmw = is_rmw;
        this->is_locked = false;
        this->is_initialized = false;
}

void OCCAction::add_read_key(uint32_t tableId, uint64_t key) 
{        
        occ_composite_key k(tableId, key, false);
        readset.push_back(k);
}

OCCAction::OCCAction(txn *txn) : translator(txn)
{
        insert_ptr = 0;
}

void OCCAction::add_write_key(uint32_t tableId, uint64_t key, bool is_rmw)
{
        occ_composite_key k(tableId, key, is_rmw);
        writeset.push_back(k);
        shadow_writeset.push_back(k);
}

void OCCAction::set_allocator(RecordBuffers *bufs)
{
        this->record_alloc = bufs;
}

void OCCAction::set_tables(Table **tables, Table **lock_tables)
{
        this->tables = tables;
        this->lock_tables = lock_tables;
}

void OCCAction::set_mgr(mcs_mgr *mgr)
{
        this->mgr = mgr;
}

uint64_t OCCAction::stable_copy(uint64_t key, uint32_t table_id, void **rec_ptr,
                                void *record_copy)
{
        volatile uint64_t *tid_ptr;
        uint32_t record_size;
        uint64_t ret, after_read;
        void *value;
        Table *tbl;

        tbl = tbl_mgr->get_table(table_id);
        assert(tbl != NULL);

        value = tbl->Get(key);
        *rec_ptr = value;      
        record_size = this->record_alloc->GetRecordSize(table_id);
        tid_ptr = (volatile uint64_t*)OCC_TIMESTAMP_PTR(value);
        while (true) {
                barrier();
                ret = *tid_ptr;
                barrier();
                if (!IS_LOCKED(ret)) {
                        if (READ_COMMITTED)
                                memcpy(record_copy, RC_VALUE_PTR(value), 
                                       record_size);
                        else
                                memcpy(record_copy, OCC_VALUE_PTR(value),
                                       record_size);

                        barrier();
                        after_read = *tid_ptr;
                        barrier();
                        if (after_read == ret)
                                break;

                }
        }
        return ret;
}

bool OCCAction::validate_single(void *value, uint64_t read_tid, bool is_rmw)
{        
        assert(!IS_LOCKED(read_tid));
        uint64_t cur_tid;
        
        if (READ_COMMITTED) {
                barrier();
                cur_tid = *((volatile uint64_t*)RC_TIMESTAMP_PTR(value));
                barrier();
        } else {
                barrier();
                cur_tid = *((volatile uint64_t*)OCC_TIMESTAMP_PTR(value));
                barrier();
        }

        if (is_rmw)
                assert(IS_LOCKED(cur_tid));
        if ((GET_TIMESTAMP(cur_tid) != read_tid) ||
            (IS_LOCKED(cur_tid) && is_rmw == false))
                return false;

        return true;
}

bool OCCAction::validate()
{
        uint32_t num_reads, num_writes, i;
        conc_table_record *iter;
        concurrent_table *tbl;
        
        if (!READ_COMMITTED) {
                num_reads = this->readset.size();
                for (i = 0; i < num_reads; ++i) {
                        assert(this->readset[i].is_initialized == true);
                        if (validate_single(this->readset[i].record_ptr, 
                                            this->readset[i].old_tid, 
                                            false) == false) {
                                return false;
                        }
                }
                num_writes = this->writeset.size();
                for (i = 0; i < num_writes; ++i) {
                        assert(this->writeset[i].is_initialized == true);
                        if (this->writeset[i].is_rmw == true && validate_single(this->writeset[i].record_ptr,
                                                                                this->writeset[i].old_tid,
                                                                                true) == false) {
                                return false;
                        }
                }        
        }

        /* Validate inserts */
        iter = inserted;
        while (iter != NULL) {
                if (validate_single(iter->inserted_record->value, 0, true) == false) {
                        return false;
                }
                iter = iter->next_in;
        }

        return true;
}

void OCCAction::create_inserts(uint32_t n_inserts)
{
        uint32_t i;
        occ_composite_key k(0, 0, false);

        k.tableId = 0;
        k.key = 0;
        k.old_tid = 0;
        k.is_rmw = false;
        k.is_locked = false;
        k.is_initialized = false;
        k.buffer = NULL;
        k.lock = NULL;
        k.record_ptr = NULL;
        
        for (i = 0; i < n_inserts; ++i) 
                inserts.push_back(k);
}

void* OCCAction::insert_ref(uint64_t key, uint32_t table_id)
{
        occ_composite_key *ins;
        conc_table_record *record;
        concurrent_table *tbl;
        bool success;        

        record = insert_mgr->get_insert_record(table_id);
        record->key = key;
        record->next = NULL;

        ins = key_allocator->get();

        ins->ins_buffer = record;
        ins->key = key;
        ins->tableId = table_id;
        
        if (READ_COMMITTED) {
                memset(record->value, 0x0, RC_RECORD_SIZE(0));
        } else {
                memset(record->value, 0x0, OCC_RECORD_SIZE(0));
        }

        record->ref_count = 0;
        tbl = tbl_mgr->get_conc_table(table_id);
        assert(tbl != NULL);
        tbl->Put(record, lck);

        if (READ_COMMITTED) {
                return RC_VALUE_PTR(record->value);
        } else {
                return OCC_VALUE_PTR(record->value);
        }
}

void OCCAction::undo_inserts()
{
}

void OCCAction::remove(__attribute__((unused)) uint64_t key, 
                       __attribute__((unused)) uint32_t table_id)
{
        assert(false);
}


void* OCCAction::write_ref(uint64_t key, uint32_t table_id)
{
        uint64_t tid;
        RecordBuffy *record;
        uint32_t i, num_writes;
        occ_composite_key *comp_key;        

        num_writes = this->writeset.size();
        comp_key = NULL;
        for (i = 0; i < num_writes; ++i) {
                if (writeset[i].key == key && writeset[i].tableId == table_id) {
                        comp_key = &writeset[i];
                        break;
                }                
        }
        assert(comp_key != NULL);
        if (comp_key->is_initialized == false) {
                record = this->record_alloc->GetRecord(table_id);
                comp_key->is_initialized = true;
                comp_key->buffer = record;
                if (writeset[i].is_rmw == true) {
                        tid = stable_copy(key, table_id, &comp_key->record_ptr, 
                                          record->value);
                        comp_key->old_tid = tid;
                }
        } 
        return comp_key->buffer->value;
}

int OCCAction::rand()
{
        return worker->gen_random();
}

uint64_t OCCAction::gen_guid()
{
        return worker->gen_guid();
}

void* OCCAction::read(uint64_t key, uint32_t table_id)
{
        uint64_t tid;
        RecordBuffy *record;
        uint32_t i, num_reads;
        occ_composite_key *comp_key;

        num_reads = this->readset.size();
        comp_key = NULL;
        for (i = 0; i < num_reads; ++i) {
                if (this->readset[i].key == key &&
                    this->readset[i].tableId == table_id) {
                        comp_key = &this->readset[i];
                        break;
                }                
        }

        if (TPCC) {                
                if (comp_key == NULL) {
                        void *temp;
                        temp = tbl_mgr->get_table(table_id)->Get(key);
                        if (READ_COMMITTED)
                                return RC_VALUE_PTR(temp);
                        else 
                                return OCC_VALUE_PTR(temp);
                }
        } else {
                assert(comp_key != NULL);
        }
        if (comp_key->is_initialized == false) {
                record = this->record_alloc->GetRecord(table_id);
                comp_key->is_initialized = true;
                comp_key->buffer = record;
                tid = stable_copy(key, table_id, &comp_key->record_ptr, record->value);
                comp_key->old_tid = tid;
        }
        return RECORD_VALUE_PTR(comp_key->buffer->value);
}

void OCCAction::check_locks()
{
        uint32_t i, num_writes;

        num_writes = writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(IS_LOCKED(*(volatile uint64_t*)writeset[i].record_ptr));
                assert(GET_LOCK_HOLDER(*(volatile uint64_t*)writeset[i].record_ptr) == cpu_id);
        }
}

void OCCAction::acquire_locks()
{
        uint32_t i, num_inserts, num_writes, table_id;
        uint64_t key;
        occ_composite_key *ins_ptrs;
        conc_table_record *record;
        mcs_struct *cur_lock;
        concurrent_table *tbl;
        bool success;        
        void *value;

        num_writes = this->writeset.size();
        std::sort(this->writeset.begin(), this->writeset.end());

        /* Writeset */
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_locked == false);
                table_id = this->writeset[i].tableId;
                key = this->writeset[i].key;                

                assert(this->writeset[i].is_initialized == true);

                if (this->writeset[i].is_rmw == false) {
                        value = tbl_mgr->get_table(table_id)->GetAlways(key);
                        this->writeset[i].record_ptr = value;
                }
                value = this->writeset[i].record_ptr;

                if (READ_COMMITTED) {
                        cur_lock = mgr->get_struct();
                        this->writeset[i].lock = cur_lock;
                        cur_lock->_tail_ptr = (volatile mcs_struct**)RC_LOCK_PTR(value);
                        mcs_mgr::lock(cur_lock);
                } else {
                        acquire_single((volatile uint64_t*)OCC_TIMESTAMP_PTR(value));
                        assert(IS_LOCKED(*(volatile uint64_t*)writeset[i].record_ptr));
                        assert(GET_LOCK_HOLDER(*(volatile uint64_t*)writeset[i].record_ptr) == cpu_id);

                }                        
                this->writeset[i].is_locked = true;
        }

        /* Inserts */
        ins_ptrs = key_allocator->get_records(&num_inserts);
        for (i = 0; i < num_inserts; ++i) {
                record = NULL;
                
                /* Logic for read committed hasn't yet been implemented. */
                //                assert(!READ_COMMITTED);                
                tbl = tbl_mgr->get_conc_table(ins_ptrs[i].tableId);
                success = tbl->Get(ins_ptrs[i].key, lck, &record);
                
                /* Must get a successful return here */
                assert(record->key == ins_ptrs[i].key);
                assert(success == true);

                if (READ_COMMITTED) {
                        cur_lock = mgr->get_struct();
                        ins_ptrs[i].lock = cur_lock;
                        cur_lock->_tail_ptr = (volatile mcs_struct**)RC_LOCK_PTR(record->value);
                        mcs_mgr::lock(cur_lock);                        
                } else {
                        acquire_single((volatile uint64_t*)OCC_TIMESTAMP_PTR(record->value));
                }
                ins_ptrs[i].record_ptr = record;
        }
}

void OCCAction::release_locks()
{
        uint32_t i, num_inserts, num_writes;
        concurrent_table *tbl;
        occ_composite_key *ins_ptrs;

        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_locked == true && 
                       this->writeset[i].is_initialized == true);
                
                if (READ_COMMITTED) {
                        mcs_mgr::unlock((mcs_struct*)writeset[i].lock);
                        this->mgr->return_struct((mcs_struct*)writeset[i].lock);
                } else {
                        release_single((volatile uint64_t*)writeset[i].record_ptr);
                }
                this->writeset[i].is_locked = false;
        }


        ins_ptrs = key_allocator->get_records(&num_inserts);
        for (i = 0; i < num_inserts; ++i) {
                assert(ins_ptrs[i].record_ptr != NULL);

                if (READ_COMMITTED) {
                        mcs_mgr::unlock((mcs_struct*)ins_ptrs[i].lock);
                        this->mgr->return_struct((mcs_struct*)ins_ptrs[i].lock);
                } else {
                        release_single((volatile uint64_t*)OCC_TIMESTAMP_PTR(((conc_table_record*)ins_ptrs[i].record_ptr)->value));
                }

                tbl = tbl_mgr->get_conc_table(ins_ptrs[i].tableId);
                if (tbl->RemoveRecord((conc_table_record*)ins_ptrs[i].record_ptr, this->lck)) {
                        insert_mgr->return_insert_record((conc_table_record*)ins_ptrs[i].record_ptr, ins_ptrs[i].tableId);
                }
                
                if (ins_ptrs[i].ins_buffer != ins_ptrs[i].record_ptr) {
                        insert_mgr->return_insert_record(ins_ptrs[i].ins_buffer, ins_ptrs[i].tableId);
                }
        }
        /*

        iter = inserted;
        i = 0;
        while (iter != NULL) {
                assert(!READ_COMMITTED);
                assert(iter->inserted_record != NULL);
                rec = iter;
                release_single((volatile uint64_t*)OCC_TIMESTAMP_PTR(rec->inserted_record->value));

                tbl = tbl_mgr->get_conc_table(iter->table_id);
                iter = iter->next_in;

                if (tbl->RemoveRecord(rec->inserted_record, this->lck)) {
                        assert(!IS_LOCKED(*OCC_TIMESTAMP_PTR(rec->inserted_record->value)));
                        insert_mgr->return_insert_record(rec->inserted_record, 
                                                         rec->table_id);
                }
                
                if (rec->inserted_record != rec) {
                        insert_mgr->return_insert_record(rec, rec->table_id);
                }
                i += 1;
        }
        */
}

void OCCAction::cleanup_single(occ_composite_key &comp_key)
{
        //        assert(comp_key.value != NULL);
        this->record_alloc->ReturnRecord(comp_key.tableId, comp_key.buffer);
        comp_key.buffer = NULL;
        comp_key.is_initialized = false;        
        
}

uint64_t OCCAction::compute_tid(uint32_t epoch, uint64_t last_tid)
{
        uint64_t max_tid, cur_tid, key;
        uint32_t num_reads, num_writes, i, table_id;
        volatile uint64_t *value;
        max_tid = CREATE_TID(epoch, 0);
        if (max_tid <  last_tid)
                max_tid = last_tid;
        assert(!IS_LOCKED(max_tid));
        num_reads = this->readset.size();
        num_writes = this->writeset.size();

        if (!READ_COMMITTED) {
                for (i = 0; i < num_reads; ++i) {
                        assert(this->readset[i].is_initialized == true);
                        cur_tid = GET_TIMESTAMP(this->readset[i].old_tid);
                        assert(!IS_LOCKED(cur_tid));
                        if (cur_tid > max_tid)
                                max_tid = cur_tid;
                }
        }
        
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_initialized == true);
                table_id = this->writeset[i].tableId;
                key = this->writeset[i].key;                

                if (READ_COMMITTED)
                        value = (volatile uint64_t*)RC_TIMESTAMP_PTR(this->writeset[i].record_ptr);
                else
                        value = (volatile uint64_t*)OCC_TIMESTAMP_PTR(this->writeset[i].record_ptr);
                assert(READ_COMMITTED || IS_LOCKED(*value));
                barrier();
                cur_tid = GET_TIMESTAMP(*value);
                barrier();
                assert(!IS_LOCKED(cur_tid));
                if (cur_tid > max_tid)
                        max_tid = cur_tid;

        }
        max_tid += 0x100;
        assert(max_tid > last_tid);
        this->tid = max_tid;        
        assert(!IS_LOCKED(max_tid));
        return max_tid;
}

bool OCCAction::run()
{
        inserted = NULL;
        return this->t->Run();
}

void OCCAction::cleanup()
{
        uint32_t i, num_writes, num_reads;
        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_locked == false);
                if (this->writeset[i].is_initialized == true)
                       cleanup_single(this->writeset[i]);
        }
        num_reads = this->readset.size();
        for (i = 0; i < num_reads; ++i) {
                if (this->readset[i].is_initialized == true)
                        cleanup_single(this->readset[i]);
        }
        
        insert_ptr = 0;
        inserted = NULL;        
}


void OCCAction::install_single_write(void *record_ptr, void *value, 
                                     mcs_struct *lck_ptr, 
                                     size_t record_size)
{
        assert(READ_COMMITTED || lck_ptr == NULL);
        assert(IS_LOCKED(this->tid) == false);

        volatile uint64_t *tid_ptr;
        tid_ptr = NULL;
        
        if (READ_COMMITTED) { 
                tid_ptr = (volatile uint64_t*)RC_TIMESTAMP_PTR(record_ptr);
                if (RC_VALUE_PTR(record_ptr) != value) {
                        acquire_single(tid_ptr);
                        memcpy(RC_VALUE_PTR(record_ptr), value, record_size);
                }
        } else {
                tid_ptr = (volatile uint64_t*)OCC_TIMESTAMP_PTR(record_ptr);
                if (OCC_VALUE_PTR(record_ptr) != value) {
                        memcpy(OCC_VALUE_PTR(record_ptr), value, record_size);
                }
        }
        
        xchgq(tid_ptr, this->tid);
        if (READ_COMMITTED) {
                mcs_mgr::unlock(lck_ptr);
                this->mgr->return_struct((mcs_struct*)lck_ptr);
        }
}

void OCCAction::install_single_insert(occ_composite_key &comp_key)
{
        assert(IS_LOCKED(this->tid) == false);
        TableRecord *rec;
        uint64_t old_tid;
        uint64_t* tid_ptr;

        rec = (TableRecord*)comp_key.record_ptr;
        tid_ptr = (uint64_t*)RECORD_TID_PTR(rec->value);
        old_tid = *tid_ptr;
        assert(GET_TIMESTAMP(old_tid) == 0);
        assert(IS_LOCKED(old_tid));
        xchgq((volatile uint64_t*)tid_ptr, this->tid);
}

void OCCAction::install_writes()
{
        uint32_t i, num_ins, num_writes;
        occ_composite_key *ins_ptrs;
        conc_table_record *record, *buffer;
        size_t record_sz;
        uint64_t lock_tid;

        /* Writes to pre-existing records */
        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_initialized == true);
                record_sz = this->record_alloc->GetRecordSize(this->writeset[i].tableId);
                if (READ_COMMITTED) {
                        install_single_write(this->writeset[i].record_ptr, 
                                             this->writeset[i].buffer->value, 
                                             (mcs_struct*)this->writeset[i].lock,
                                             record_sz);
                } else {
                        install_single_write(this->writeset[i].record_ptr, 
                                             this->writeset[i].buffer->value, 
                                             NULL,
                                             record_sz);
                }
                this->writeset[i].is_locked = false;
        }

        /* Inserts */
        ins_ptrs = key_allocator->get_records(&num_ins);
        for (i = 0; i < num_ins; ++i) {
                record_sz = this->record_alloc->GetRecordSize(ins_ptrs[i].tableId);
                record = (conc_table_record*)ins_ptrs[i].record_ptr;
                buffer = ins_ptrs[i].ins_buffer;
                if (READ_COMMITTED) {
                        install_single_write(record->value,
                                             RC_VALUE_PTR(buffer->value),
                                             (mcs_struct*)ins_ptrs[i].lock,
                                             record_sz);
                        
                } else {
                        install_single_write(record->value,
                                             OCC_VALUE_PTR(buffer->value),
                                             NULL,
                                             record_sz);
                }

                if (record != buffer) 
                        insert_mgr->return_insert_record(buffer, ins_ptrs[i].tableId);
        }

        /*
        iter = inserted;
        while (iter != NULL) {                


                if (READ_COMMITTED) {
                        install_single_write(iter->inserted_record->value, 
                                             RC_VALUE_PTR(iter->value),
                                             NULL,
                                             record_sz);
                } else {
                        install_single_write(iter->inserted_record->value, 
                                             OCC_VALUE_PTR(iter->value),
                                             NULL,
                                             record_sz);
                }
                temp = iter;
                iter = iter->next_in;
                if (temp != temp->inserted_record)
                        insert_mgr->return_insert_record(temp, temp->table_id);
        }
        */
}
