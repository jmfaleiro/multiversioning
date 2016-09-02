#include <db.h>
#include <cassert>

#ifdef 	RUNTIME_PIPELINING

void translator::release_piece(__attribute__((unused)) uint32_t piece_num)
{
}

void txn::release_piece(uint32_t piece_num)
{
        trans->release_piece(piece_num);
}

#endif

txn::txn()
{
        this->trans = NULL;
}

void txn::set_translator(translator *trans)
{
        assert(this->trans == NULL);
        this->trans = trans;
}

void* txn::get_write_ref(uint64_t key, uint32_t table_id)
{
        return trans->write_ref(key, table_id);
}

void* txn::get_read_ref(uint64_t key, uint32_t table_id)
{
        return trans->read(key, table_id);
}

void* txn::insert_record(uint64_t key, uint32_t table_id)
{
        return trans->insert_ref(key, table_id);
}

void txn::remove_record(uint64_t key, uint32_t table_id)
{
        trans->remove(key, table_id);
}

uint32_t txn::num_reads()
{
        return 0;
}

uint32_t txn::num_writes()
{
        return 0;
}

uint32_t txn::num_rmws()
{
        return 0;
}

void txn::get_reads(__attribute__((unused)) struct big_key *array)
{
        return;
}

void txn::get_writes(__attribute__((unused)) struct big_key *array)
{
        return;
}

void txn::get_rmws(__attribute__((unused)) struct big_key *array)
{
        return;
}

int txn::txn_rand()
{
        return trans->rand();
}

uint64_t txn::guid()
{
        return trans->gen_guid();
}
