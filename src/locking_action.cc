#include <locking_action.h>
#include <algorithm>
#include <util.h>
#include <eager_worker.h>
#include <lock_manager.h>
#include <locking_record.h>

#define RECORD_VALUE_PTR(rec_ptr) ((void*)&(((uint64_t*)rec_ptr)[1]))

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
                read_value = LOCKING_VALUE_PTR(k->value);
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
                return LOCKING_VALUE_PTR(tables->get_table(table_id)->Get(key));
        //        assert(index != -1 && index < this->readset.size());
        k = &this->readset[index];
        return LOCKING_VALUE_PTR(k->value);
        /*
        if (k->value == NULL) 
                k->value = lookup(k);
        return k->value;
        */
}

void* locking_action::insert_ref(uint64_t key, uint32_t table_id)
{
        TableRecord *record;
        concurrent_table *tbl;
        bool success;        

        record = insert_mgr->get_insert_record(table_id);
        record->key = key;
        record->next = NULL;
        tbl = tables->get_conc_table(table_id);
        assert(tbl != NULL);
        success = tbl->Put(record, lck);
        assert(success == true);
        return record->value;
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

#ifdef 	RUNTIME_PIPELINING

        this->read_release = 0;
        this->write_release = 0;

#endif

        this->prepared = true;
}



bool locking_action::Run()
{
        bool commit;
        commit = this->t->Run();
        //        commit_writes(commit);
        return commit;
}

#ifdef RUNTIME_PIPELINING

void locking_action::release_piece(uint32_t piece_num)
{
        lock_mgr->ReleaseTable(this, piece_num);
}

#endif 
