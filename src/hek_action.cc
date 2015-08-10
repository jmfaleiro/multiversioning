#include <hek_action.h>
#include <small_bank.h>
#include <hek.h>

extern uint64_t *record_sizes;

using namespace hek_small_bank;

static hek_key create_blank_key()
{
        hek_key ret = {
                0,		/* key */
                0,		/* table_id */
                NULL,		/* txn */
                NULL,		/* value */
                NULL,		/* next */
                NULL,		/* table_ptr */
                0,		/* time */                
                0,		/* prev_ts */
                false,		/* written */
                false,		/* is_rmw */
                0,		/* txn_ts */
                false,
        };
        return ret;
}

hek_action::hek_action(txn *t) : translator(t)
{
        this->readonly = false;
        
}

hek_status hek_action::Run()
{
        hek_status status;
        
        status.validation = true;
        status.commit = this->t->Run();
        return status;
}

void* hek_action::read(uint64_t key, uint32_t table_id)
{
        uint32_t num_reads, i;
        uint64_t ts, *begin_ptr, *txn_ts;
        struct hek_record *read_record;
        
        void *ret = NULL;
        ts = HEK_TIME(this->begin);
        num_reads = this->readset.size();
        for (i = 0; i < num_reads; ++i) {
                if (this->readset[i].table_id == table_id && 
                    this->readset[i].key == key) {
                        if (this->readset[i].value == NULL) {
                                begin_ptr = &this->readset[i].time;
                                txn_ts = &this->readset[i].txn_ts;
                                read_record = 
                                        tables[table_id]->get_version(key, ts, 
                                                                      begin_ptr,
                                                                      txn_ts);
                                this->readset[i].value = read_record;
                        }
                        ret = this->readset[i].value->value;
                        break;
                }
        }
        assert(ret != NULL);
        return ret;
}

void* hek_action::write_ref(uint64_t key, uint32_t table_id)
{
        uint32_t num_writes, i;
        uint64_t ts, *begin_ptr, *txn_ts;
        struct hek_record *read_record, *write_record;
        
        void *ret = NULL;
        ts = HEK_TIME(this->begin);
        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                if (this->writeset[i].table_id == table_id && 
                    this->writeset[i].key == key) {
                        if (this->writeset[i].init == false) {
                                write_record = this->writeset[i].value;
                                if (write_record == NULL)
                                        write_record = 
                                                this->worker->get_new_record(table_id);
                                if (this->writeset[i].is_rmw == true) {
                                        begin_ptr = &this->writeset[i].time;
                                        txn_ts = &this->writeset[i].txn_ts;
                                        read_record = 
                                                tables[table_id]->get_version(key, ts, 
                                                                              begin_ptr,
                                                                              txn_ts);
                                        write_record->key = key;
                                        memcpy(write_record->value, read_record->value, 
                                               record_sizes[table_id]);
                                }
                                this->writeset[i].value = write_record;
                        }
                        ret = this->writeset[i].value->value;
                        break;
                }

        }
        assert(ret != NULL);
        return ret;
}

void hek_action::reset()
{
        uint32_t num_reads, num_writes, i, table_id;
        hek_key *key;
        hek_record *record;
        
        num_reads = this->readset.size();
        for (i = 0; i < num_reads; ++i) 
                this->readset[i].value = NULL;
        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                if (this->writeset[i].written == true) {
                        this->writeset[i].written = false;
                        key = &this->writeset[i];
                        record = key->value;
                        table_id = key->table_id;
                        assert(record != NULL);
                        record = key->value;
                        tables[table_id]->remove_version(record);
                } 
                this->writeset[i].init = false;
        }
}

void hek_action::add_read_key(uint32_t table_id, uint64_t key)
{
        hek_key k = create_blank_key();
        k.key = key;
        k.table_id = table_id;
        this->readset.push_back(k);
}

void hek_action::add_write_key(uint32_t table_id, uint64_t key, bool is_rmw)
{
        hek_key k = create_blank_key();
        k.key = key;
        k.table_id = table_id;
        k.is_rmw = is_rmw;
        this->writeset.push_back(k);
}
