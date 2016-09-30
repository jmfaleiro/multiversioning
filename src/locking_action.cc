#include <locking_action.h>
#include <algorithm>
#include <util.h>
#include <eager_worker.h>
#include <lock_manager.h>
#include <locking_record.h>
#include <pipelined_record.h>

#define RECORD_VALUE_PTR(rec_ptr) ((void*)&(((uint64_t*)rec_ptr)[1]))

extern uint32_t cc_type;

void* lck_key_allocator::operator new(std::size_t sz, int cpu)
{
        return alloc_mem(sz, cpu);
}

lck_key_allocator::lck_key_allocator(uint32_t sz, int cpu)
{
        _free_list = (locking_key*)alloc_mem(sizeof(locking_key)*sz, cpu);
        memset(_free_list, 0x0, sz);
        _cursor = 0;
        _sz = sz;
}

locking_key* lck_key_allocator::get()
{
        assert(_cursor < _sz - 1);
        _cursor += 1;
        return &_free_list[_cursor-1];
}

void lck_key_allocator::reset()
{
        _cursor = 0;        
}

locking_key::locking_key(uint64_t key, uint32_t table_id, bool is_write)
{
        this->key = key;
        this->table_id = table_id;
        this->is_write = is_write;
        this->dependency = NULL;
        this->is_held = false;
        this->latch = NULL;
        this->prev = NULL;
        this->next = NULL;
        this->is_initialized = false;
        this->value = NULL;
}

locking_key::locking_key()
{
}

int locking_action::rand()
{
        return worker->gen_random();
}

uint64_t locking_action::gen_guid()
{
        return worker->gen_guid();
}

locking_action::locking_action(txn *txn) : translator(txn)
{
        this->worker = NULL;
        this->prepared = false;
        this->read_index = 0;
        this->write_index = 0;
        this->bufs = NULL;
}

void locking_action::add_write_key(uint64_t key, uint32_t table_id)
{
        locking_key to_add(key, table_id, true);
        to_add.dependency = this;
        this->writeset.push_back(to_add);
}

void locking_action::add_read_key(uint64_t key, uint32_t table_id)
{
        locking_key to_add(key, table_id, false);
        to_add.dependency = this;
        this->readset.push_back(to_add);
}

void* locking_action::lookup(locking_key *k)
{
        if (k->is_write == true) 
                return tables->get_table(k->table_id)->GetAlways(k->key);
        else
                return tables->get_table(k->table_id)->Get(k->key);
}

int locking_action::find_key(uint64_t key, uint32_t table_id,
                             std::vector<locking_key> key_list)
{
        uint32_t i, num_keys;
        int ret;

        ret = -1;
        num_keys = key_list.size();
        for (i = 0; i < num_keys; ++i) {
                if (key_list[i].key == key &&
                    key_list[i].table_id == table_id) {
                        ret = i;
                        break;
                }
        }
        return ret;
}

void locking_action::commit_writes(bool commit)
{
        locking_key *k;
        uint32_t i, num_writes, record_size;
        void *value;

        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                k = &this->writeset[i];
                if (k->value != NULL) {
                        if (commit) {
                                value = lookup(k);
                                record_size = tables->get_table(k->table_id)->RecordSize();                        
                                memcpy(value, RECORD_VALUE_PTR(k->value), record_size);
                        }
                        this->bufs->ReturnRecord(k->table_id, k->value);
                        k->value = NULL;
                }
        }
}

void* locking_action::write_ref(uint64_t key, uint32_t table_id)
{
        locking_key *k;
        int index;
        void *read_value;
        uint32_t record_size;

        index = find_key(key, table_id, this->writeset);
        assert(index != -1 && index < this->writeset.size());
        k = &this->writeset[index];
        if (k->buf == NULL) {
                if (cc_type == 1)
                        read_value = LOCKING_VALUE_PTR(k->value);
                else if (cc_type == 5) 
                        read_value = PIPELINED_VALUE_PTR(k->value);
                else 
                        assert(false);


                k->buf = this->bufs->GetRecord(table_id);
                record_size = this->bufs->GetRecordSize(table_id);
                memcpy(k->buf, read_value, record_size);
        }
        return k->buf;
        /*
        if (k->value == NULL) {
                read_value = lookup(k);
                k->value = this->bufs->GetRecord(table_id);
                record_size = this->tables->get_table(table_id)->RecordSize();
                memcpy(RECORD_VALUE_PTR(k->value), read_value, record_size);
        }
        return RECORD_VALUE_PTR(k->value);
        */
}

void* locking_action::read(uint64_t key, uint32_t table_id)
{
        //        return tables->get_table(table_id)->Get(key);
        locking_key *k;
        int index;
        
        index = find_key(key, table_id, this->readset);
        if (index == -1)
                if (cc_type == 1) {
                        return LOCKING_VALUE_PTR(tables->get_table(table_id)->Get(key));
                } else if (cc_type == 5) {
                        return PIPELINED_VALUE_PTR(tables->get_table(table_id)->Get(key));
                } else {
                        assert(false);
                        return NULL;
                }


        //        assert(index != -1 && index < this->readset.size());
        k = &this->readset[index];
        if (cc_type == 1) {
                return LOCKING_VALUE_PTR(k->value);
        } else if (cc_type == 5) {
                return PIPELINED_VALUE_PTR(k->value);
        } else {
                assert(false);
                return NULL;
        }

        /*
        if (k->value == NULL) 
                k->value = lookup(k);
        return k->value;
        */
}

void* locking_action::insert_ref(uint64_t key, uint32_t table_id)
{
        conc_table_record *record;
        concurrent_table *tbl;
        bool success;        
        locking_key *wrapper;
        
        /* Get a wrapper to remember a reference to the inserted record */
        wrapper = key_alloc->get();        
        wrapper->next = inserted;
        inserted = wrapper;
        
        /* Allocate a record to insert, and remember a reference to it */
        record = insert_mgr->get_insert_record(table_id);
        record->key = key;
        record->next = NULL;
        wrapper->value = record;
        
        /* Insert the new record */
        acquire_writer((mcs_rw::mcs_rw_lock*)record->value, &wrapper->lock_node);
        tbl = tables->get_conc_table(table_id);
        assert(tbl != NULL);
        success = tbl->Put(record, lck);
        assert(success == true);
        return record->value;
}

/* Called at the end of a txn. Release write locks on inserted records */
void locking_action::finish_inserts()
{
        assert(this->status == COMPLETE);
        locking_key *iter, *temp;
        mcs_rw::mcs_rw_lock *record_ptr;

        iter = inserted;
        while (iter != NULL) {
                record_ptr = (mcs_rw::mcs_rw_lock*)((conc_table_record*)iter->value)->value;
                release_writer(record_ptr, &iter->lock_node);
                temp = iter;
                iter = iter->next;
        }
        key_alloc->reset();
}

void locking_action::remove(__attribute__((unused)) uint64_t key, 
                            __attribute__((unused)) uint32_t table_id)
{
        assert(false);
}

void locking_action::prepare()
{
        if (this->prepared == true) 
                return;
        std::sort(this->readset.begin(), this->readset.end());
        std::sort(this->writeset.begin(), this->writeset.end());
        barrier();
        this->num_dependencies = 0;
        barrier();

        this->prepared = true;
}

bool locking_action::Run()
{
        assert(this->status == UNEXECUTED);
        bool commit;
        commit = this->t->Run();
        //        commit_writes(commit);
        xchgq((volatile uint64_t*)&this->status, COMPLETE);        
        
        return commit;
}

