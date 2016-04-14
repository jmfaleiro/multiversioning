
#include <occ_action.h>
#include <algorithm>
#include <occ.h>

static bool try_acquire_single(volatile uint64_t *lock_ptr)
{
        volatile uint64_t cmp_tid, locked_tid;
        barrier();
        cmp_tid = *lock_ptr;
        barrier();
        if (IS_LOCKED(cmp_tid))
                return false;        
        locked_tid = (cmp_tid | 1);
        return cmp_and_swap(lock_ptr, cmp_tid, locked_tid);
}

static void acquire_single(volatile uint64_t *lock_ptr)
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
//                         if (READ_COMMITTED) {
//                                 barrier();
//                                 for (i = 0; i < 100; ++i) 
//                                         single_work();
//                                 barrier();
//                                 do_pause();
//                         } else {
                         temp = backoff;
                         while (temp-- > 0)
                                 single_work();
                         backoff = backoff*2;
                         //                        }
                }
        }
}

static void release_single(volatile uint64_t *lock_word)
{
        uint64_t old_tid, xchged_tid;
        
        old_tid = *lock_word;
        assert(IS_LOCKED(old_tid));
        old_tid = GET_TIMESTAMP(old_tid);
        xchged_tid = xchgq(lock_word, old_tid);
        assert(IS_LOCKED(xchged_tid));
        assert(GET_TIMESTAMP(xchged_tid) == old_tid);
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

void* occ_composite_key::GetValue() const
{
        uint64_t *temp = (uint64_t*)value;
        return &temp[1];
}

void* occ_composite_key::StartRead()
{

        volatile uint64_t *tid_ptr;
        tid_ptr = (volatile uint64_t*)value;
        while (true) {
                barrier();
                this->old_tid = *tid_ptr;
                barrier();
                if (!IS_LOCKED(this->old_tid))
                        break;
        }
        return (void*)&tid_ptr[1];

        //        return (void*)&((uint64_t*)value)[1];
}

bool occ_composite_key::FinishRead()
{
        return true;
        /*
        assert(!IS_LOCKED(this->old_tid));
        volatile uint64_t tid;
        bool is_valid = false;
        barrier();        
        tid = *(volatile uint64_t*)value;
        barrier();
        is_valid = (tid == this->old_tid);
        assert(!is_valid || !IS_LOCKED(tid));
        return is_valid;
                //        return true;
                */
}

uint64_t occ_composite_key::GetTimestamp()
{
        return old_tid;        
}

bool occ_composite_key::ValidateRead()
{
        assert(!IS_LOCKED(old_tid));
        volatile uint64_t *version_ptr;
        volatile uint64_t cur_tid;
        version_ptr = (volatile uint64_t*)value;
        barrier();
        cur_tid = *version_ptr;
        barrier();
        if ((GET_TIMESTAMP(cur_tid) != old_tid) ||
            (IS_LOCKED(cur_tid) && !is_rmw))
                return false;
        return true;
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

bool OCCAction::stable_copy(uint64_t key, uint32_t table_id, void **rec_ptr,
                            void *record_copy, uint64_t *tid)
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
        record_size = REAL_RECORD_SIZE(tbl->RecordSize());
        tid_ptr = (volatile uint64_t*)value;
        while (true) {
                barrier();
                ret = *tid_ptr;
                barrier();
                if (!IS_LOCKED(ret)) {
                        memcpy(RECORD_VALUE_PTR(record_copy),
                               RECORD_VALUE_PTR(value), record_size);
                        barrier();
                        after_read = *tid_ptr;
                        barrier();
                        if (after_read == ret) {
                                *tid = ret;
                                return true;
                        } else if (READ_COMMITTED) {
                                continue;
                        } else {
                                return false;
                        }
                }
        }
}

bool OCCAction::validate_single(occ_composite_key &comp_key)
{
        assert(!IS_LOCKED(comp_key.old_tid));
        void *value;
        volatile uint64_t *version_ptr;
        uint64_t cur_tid;
        
        value = comp_key.record_ptr;
        //        value = tables[comp_key.tableId]->Get(comp_key.key);
        version_ptr = (volatile uint64_t*)value;
        barrier();
        cur_tid = *version_ptr;
        barrier();

        if ((GET_TIMESTAMP(cur_tid) != comp_key.old_tid) ||
            (IS_LOCKED(cur_tid) && !comp_key.is_rmw))
                return false;
        return true;
}

bool OCCAction::validate()
{
        uint32_t num_reads, num_writes, i;

        num_reads = this->readset.size();
        for (i = 0; i < num_reads; ++i) {
                assert(this->readset[i].is_initialized == true);
                if (validate_single(this->readset[i]))
                        return false;
        }
        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_initialized == true);
                if (this->writeset[i].is_rmw)
                        if (validate_single(this->writeset[i]))
                                return false;
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
        k.value = NULL;
        k.lock = NULL;
        k.record_ptr = NULL;
        
        for (i = 0; i < n_inserts; ++i) 
                inserts.push_back(k);
}

bool OCCAction::insert_ref(uint64_t key, uint32_t table_id, void **val)
{
        assert(insert_ptr < inserts.size());
        void *value;
        conc_table_record *record;
        concurrent_table *tbl;
        mcs_struct *lock_struct;
        bool success;        

        record = insert_mgr->get_insert_record(table_id);
        value = record->value;
        record->key = key;
        record->next = NULL;
        record->tid = (uint64_t)worker;
        inserts[insert_ptr].record_ptr = record;
        inserts[insert_ptr].tableId = table_id;
        inserts[insert_ptr].key = key;
       
        /* Do the actual insert */
        lock_struct = mgr->get_struct();
        *RECORD_TID_PTR(record->value) = 0;
        barrier();
        acquire_single(RECORD_TID_PTR(value));
        assert(IS_LOCKED(*RECORD_TID_PTR(record->value)));
        tbl = tbl_mgr->get_conc_table(table_id);
        assert(tbl != NULL);
        success = tbl->Put(record, lock_struct);
        mgr->return_struct(lock_struct);
        if (success == false) {
                *RECORD_TID_PTR(record->value) = 0;
                barrier();
                insert_mgr->return_insert_record(record, table_id);
                return false;
        } 

        insert_ptr += 1;
        *val = RECORD_VALUE_PTR(value);
        return true;
}

void OCCAction::undo_inserts()
{
        uint32_t i, table_id;
        conc_table_record *record;
        concurrent_table *tbl;
        mcs_struct *lock_struct;

        for (i = 0; i < insert_ptr; ++i) {
                //                std::cerr << "Here!\n";
                record = (conc_table_record*)inserts[i].record_ptr;
                table_id = inserts[i].tableId;                
                tbl = tbl_mgr->get_conc_table(table_id);
                assert(IS_LOCKED(*RECORD_TID_PTR(record->value)));
                assert(GET_TIMESTAMP(*RECORD_TID_PTR(record->value)) == 0);

                assert(tbl != NULL);
                lock_struct = mgr->get_struct();

                /* Remove the record from the index */
                tbl->Remove(record, lock_struct);
        
                /* Return the record back to the allocator */
                *RECORD_TID_PTR(record->value) = 0;
                barrier();
                insert_mgr->return_insert_record(record, table_id);
                mgr->return_struct(lock_struct);
        }
}

bool OCCAction::remove(__attribute__((unused)) uint64_t key, 
                       __attribute__((unused)) uint32_t table_id)
{
        assert(false);
}


bool OCCAction::write_ref(uint64_t key, uint32_t table_id, void **val)
{
        uint64_t tid;
        void *record;
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
                comp_key->value = record;
                if (writeset[i].is_rmw == true) {
                        if (stable_copy(key, table_id, &comp_key->record_ptr, 
                                        record, &tid) == true)
                                comp_key->old_tid = tid;
                        else 
                                return false;
                }
        } 
        *val = RECORD_VALUE_PTR(comp_key->value);
        return true;
}

int OCCAction::rand()
{
        return worker->gen_random();
}

uint64_t OCCAction::gen_guid()
{
        return worker->gen_guid();
}

bool OCCAction::read(uint64_t key, uint32_t table_id, void **val)
{
        uint64_t tid;
        void *record;
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
        assert(comp_key != NULL);
        if (comp_key->is_initialized == false) {
                record = this->record_alloc->GetRecord(table_id);
                comp_key->is_initialized = true;
                comp_key->value = record;
                if (stable_copy(key, table_id, &comp_key->record_ptr, record, 
                                &tid) == true) 
                        comp_key->old_tid = tid;
                else 
                        return false;
        }
        *val = RECORD_VALUE_PTR(comp_key->value);
        return true;
}

void OCCAction::acquire_locks()
{
        uint32_t i, num_writes, table_id;
        uint64_t key;
        void *value;
        mcs_struct *cur_lock;

        num_writes = this->writeset.size();
        std::sort(this->writeset.begin(), this->writeset.end());

        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_locked == false);
                table_id = this->writeset[i].tableId;
                key = this->writeset[i].key;                
                if (READ_COMMITTED) {
                        value = this->lock_tables[table_id]->GetAlways(key);
                        cur_lock = mgr->get_struct();
                        this->writeset[i].lock = cur_lock;
                        cur_lock->_tail_ptr = (volatile mcs_struct**)value;
                        mcs_mgr::lock(cur_lock);
                        this->writeset[i].is_locked = true;
                } else { 
                        assert(this->writeset[i].is_initialized == true);
                        if (this->writeset[i].is_rmw == false) {
                                value = tbl_mgr->get_table(table_id)->GetAlways(key);
                                this->writeset[i].record_ptr = value;
                        }
                        value = this->writeset[i].record_ptr;
                        acquire_single((volatile uint64_t*)value);
                        this->writeset[i].is_locked = true;
                }
        }
}

void OCCAction::release_locks()
{
        uint32_t i, num_writes;//, table_id;
        //        uint64_t key;
        //        void *value;

        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_locked == true && 
                       this->writeset[i].is_initialized == true);
                release_single((volatile uint64_t*)writeset[i].record_ptr);
                this->writeset[i].is_locked = false;
        }
}

void OCCAction::cleanup_single(occ_composite_key &comp_key)
{
        //        assert(comp_key.value != NULL);
        this->record_alloc->ReturnRecord(comp_key.tableId, comp_key.value);
        comp_key.value = NULL;
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
                value = (volatile uint64_t*)tbl_mgr->get_table(table_id)->GetAlways(key);
                assert(READ_COMMITTED || IS_LOCKED(*value));
                barrier();
                cur_tid = GET_TIMESTAMP(*value);
                barrier();
                assert(!IS_LOCKED(cur_tid));
                if (cur_tid > max_tid)
                        max_tid = cur_tid;

        }
        max_tid += 0x10;
        this->tid = max_tid;
        assert(!IS_LOCKED(max_tid));
        return max_tid;
}

bool OCCAction::run()
{
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
}

void OCCAction::install_single_write(occ_composite_key &comp_key)
{
        assert(IS_LOCKED(this->tid) == false);

        void *value;
        uint64_t old_tid;
        uint32_t record_size;
        Table *tbl;

        tbl = tbl_mgr->get_table(comp_key.tableId);
        record_size = tbl->RecordSize();
        
        if (READ_COMMITTED) {
                value = tbl->GetAlways(comp_key.key);
                acquire_single((volatile uint64_t*)value);
        } else {
                value = comp_key.record_ptr;
        }
        old_tid = *(uint64_t*)value;
        assert(IS_LOCKED(old_tid) == true);
        memcpy(RECORD_VALUE_PTR(value), RECORD_VALUE_PTR(comp_key.value),
               record_size - sizeof(uint64_t));
        xchgq((volatile uint64_t*)value, this->tid);
        if (READ_COMMITTED) {
                mcs_mgr::unlock((mcs_struct*)comp_key.lock);
                this->mgr->return_struct((mcs_struct*)comp_key.lock);
        }
        comp_key.is_locked = false;
}

void OCCAction::install_single_insert(occ_composite_key &comp_key)
{
        assert(IS_LOCKED(this->tid) == false);
        conc_table_record *rec;
        uint64_t old_tid;
        uint64_t* tid_ptr;

        rec = (conc_table_record*)comp_key.record_ptr;
        tid_ptr = (uint64_t*)RECORD_TID_PTR(rec->value);
        old_tid = *tid_ptr;
        assert(GET_TIMESTAMP(old_tid) == 0);
        assert(IS_LOCKED(old_tid));
        xchgq((volatile uint64_t*)tid_ptr, this->tid);
}

void OCCAction::install_writes()
{
        uint32_t i, num_writes;
        
        /* Writes to pre-existing records */
        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_initialized == true);
                install_single_write(this->writeset[i]);       
        }

        /* Inserts */
        for (i = 0; i < insert_ptr; ++i) 
                install_single_insert(this->inserts[i]);
}
