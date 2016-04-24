#include <ycsb.h>
#include <cassert>
#include <string.h>

ycsb_insert::ycsb_insert(uint64_t start, uint64_t end)
{
        assert(start < end);
        this->start = start;
        this->end = end;
}

void ycsb_insert::gen_rand(char *array)
{
        uint32_t num_words, i, *int_array;
        
        num_words = YCSB_RECORD_SIZE / sizeof(uint32_t);
        int_array = (uint32_t*)array;
        for (i = 0; i < num_words; ++i) 
                int_array[i] = (uint32_t)txn_rand();
}

bool ycsb_insert::Run()
{
        uint64_t i;
        char rand_array[YCSB_RECORD_SIZE], *record_ptr;

        for (i = this->start; i < this->end; ++i) {
                gen_rand(rand_array);
                record_ptr = (char*)get_write_ref(i, 0);
                memcpy(record_ptr, rand_array, YCSB_RECORD_SIZE);
        }
        return true;
}

uint32_t ycsb_insert::num_writes()
{
        return (uint32_t)(end - start);
}

void ycsb_insert::get_writes(struct big_key *array)
{
        uint64_t i;
        for (i = this->start; i < this->end; ++i) {
                array[i-this->start].key = i;
                array[i-this->start].table_id = 0;
        }
}

ycsb_readonly::ycsb_readonly(vector<uint64_t> reads)
{
        uint32_t num_reads, i;
        
        num_reads = reads.size();
        for (i = 0; i < num_reads; ++i)
                this->reads.push_back(reads[i]);
}

bool ycsb_readonly::Run()
{
        char buf[1000], *val;
        uint32_t num_reads, i, j;
        uint64_t *buf_ptr, total;

        memset(buf, 0x0, 1000);
        num_reads = reads.size();
        for (i = 0; i < num_reads; ++i) {
                val = (char*)get_read_ref(reads[i], 0);
                for (j = 0; j < 10; ++j) {
                        buf_ptr = (uint64_t*)&buf[j*100];
                        *buf_ptr += *(uint64_t*)(&val[j*100]);
                }
        }
        
        total = 0;
        for (j = 0; j < 10; ++j) {
                buf_ptr = (uint64_t*)&buf[j*100];
                total += *buf_ptr;
        }
        accumulated = total;
        return true;
}

uint32_t ycsb_readonly::num_reads()
{
        return this->reads.size();
}

void ycsb_readonly::get_reads(struct big_key *array)
{
        uint32_t i, num_reads;
        struct big_key k;

        k.table_id = 0;
        num_reads = this->reads.size();
        for (i = 0; i < num_reads; ++i) {
                k.key = this->reads[i];
                array[i] = k;
        }
        return;
}

ycsb_rmw::ycsb_rmw(vector<uint64_t> reads, vector<uint64_t> writes)
{
        uint32_t num_reads, num_writes, i;

        num_reads = reads.size();
        num_writes = writes.size();
        for (i = 0; i < num_reads; ++i) 
                this->reads.push_back(reads[i]);
        for (i = 0; i < num_writes; ++i) 
                this->writes.push_back(writes[i]);
}

uint32_t ycsb_rmw::num_reads()
{
        return this->reads.size();
}

uint32_t ycsb_rmw::num_rmws()
{
        return this->writes.size();
}

void ycsb_rmw::get_reads(struct big_key *array)
{
        uint32_t num_reads, i;
        struct big_key k;

        k.table_id = 0;
        num_reads = this->reads.size();
        for (i = 0; i < num_reads; ++i) {
                k.key = this->reads[i];
                array[i] = k;
        }
        return;
}

void ycsb_rmw::get_rmws(struct big_key *array)
{
        uint32_t num_rmws, i;
        struct big_key k;

        k.table_id = 0;
        num_rmws = this->writes.size();
        for (i = 0; i < num_rmws; ++i) {
                k.key = this->writes[i];
                array[i] = k;
        }
        return;
}

bool ycsb_rmw::Run()
{
        uint32_t i, j, num_reads, num_writes;
        uint64_t counter;
        char *field_ptr, *write_ptr;

        num_reads = this->reads.size();
        num_writes = this->writes.size();

        /* Accumulate each field of records in the readset into "counter". */
        counter = 0;
        for (i = 0; i < num_reads; ++i) {
                field_ptr = (char*)get_read_ref(reads[i], 0);
                for (j = 0; j < 10; ++j)
                        counter += *((uint64_t*)&field_ptr[j*100]);
        }

        /* Perform an RMW operation on each element of the writeset. */
        for (i = 0; i < num_writes; ++i) {
                write_ptr = (char*)get_write_ref(writes[i], 0);
                for (j = 0; j < 10; ++j)
                        *((uint64_t*)&write_ptr[j*100]) += j+1+counter;
        }
        return true;
}

split_ycsb_read::split_ycsb_read(uint32_t *acc_array, vector<uint64_t> reads)
{
        uint32_t i, sz;

        _accumulated = acc_array;
        sz = reads.size();
        for (i = 0; i < sz; ++i) 
                _reads.push_back(reads[i]);
}

uint32_t split_ycsb_read::num_reads()
{
        return _reads.size();
}

void split_ycsb_read::get_reads(big_key *array)
{
        uint32_t i, sz;
        
        sz = _reads.size();
        //        assert(sz == 10);
        for (i = 0; i < sz; ++i) {
                array[i].key = _reads[i];
                array[i].table_id = 0;
        }                
}

bool split_ycsb_read::Run()
{
        uint32_t i, nreads, total;
        char *record;

        total = 0;
        nreads = _reads.size();
        for (i = 0; i < nreads; ++i) {
                record = (char*)get_read_ref(_reads[i], 0);
                total += *(uint32_t*)record;
                  
        }
        *_accumulated = total;
        return true;
}

void split_ycsb_read::process_record(uint32_t *acc, uint64_t key)
{
        char *val;
        uint32_t i, nfields;

        val = (char*)get_read_ref(key, 0);
        nfields = 10;
        for (i = 0; i < nfields; ++i) 
                acc[i] += *(uint32_t*)&val[i*100];
}

split_ycsb_acc::split_ycsb_acc(vector<split_ycsb_read*> read_txns)
{
        uint32_t i, ntxns;
        
        _accumulated = NULL;
        ntxns = read_txns.size();
        for (i = 0; i < ntxns; ++i) 
                _read_txns.push_back(read_txns[i]);        
}

bool split_ycsb_acc::Run()
{
        uint32_t i, j, ntxns;
        uint32_t *temp;
        
        _accumulated = _read_txns[0]->_accumulated;
        //        accumulated = &(_read_txns[0]->_accumulated);
        ntxns = _read_txns.size();
        for (i = 1; i < ntxns; ++i) {
                _accumulated[0] += _accumulated[i];
                //                temp = _accumulatr_read_txns[i]->_accumulated;
                //                *_accumulated += temp;
                //                for (j = 0; j < 10; ++j) 
                //                        _read_txns[0]->_accumulated[j] += temp[j];
                //                        (*_accumulated)[j] += temp[j];
        }
        return true;
}

split_ycsb_update::split_ycsb_update(uint32_t *accumulated,
                                     uint32_t nreads,
                                     vector<uint64_t> writes)
{
        uint32_t i, nwrites;

        _nreads = nreads;
        _accumulated = accumulated;
        nwrites = writes.size();
        for (i = 0; i < nwrites; ++i) 
                _writes.push_back(writes[i]);
}

uint32_t split_ycsb_update::num_rmws()
{
        return _writes.size();
}

void split_ycsb_update::get_rmws(big_key *arr)
{
        uint32_t i, nwrites;
        
        nwrites = _writes.size();
        for (i = 0; i < nwrites; ++i) {
                arr[i].key = _writes[i];
                arr[i].table_id = 0;
        }
}

bool split_ycsb_update::Run()
{
        uint32_t i, nwrites, total;
        char *record;

        total = *_accumulated;//0;
        //        for (i = 0; i < _nreads; ++i) 
        //                total += _accumulated[i];
        
        nwrites = _writes.size();
        for (i = 0; i < nwrites; ++i) {
                record = (char*)get_write_ref(_writes[i], 0);
                *(uint32_t*)record += total;
        }
        return true;
}

void split_ycsb_update::get_values()
{
        /*
        uint32_t i, j;
        split_ycsb_read *read_txn;

        for (i = 0; i < nreads; ++i) {
                read_txn = _reads[i];
                for (j = 0; j < 10; ++j) 
                        _accumulated[j] += read_txn->_accumulated[j];
        }
        */
}


ycsb_update::ycsb_update(vector<uint64_t> writes, uint64_t *updates)
{
        uint32_t i, sz;

        memcpy(_updates, updates, sizeof(uint64_t)*10);
        
        sz = writes.size();
        for (i = 0; i < sz; ++i) 
                _writes.push_back(writes[i]);
}

uint32_t ycsb_update::num_rmws()
{
        return _writes.size();
}

void ycsb_update::get_rmws(big_key *array)
{
        uint32_t i, sz;
        
        sz = _writes.size();
        for (i = 0; i < sz; ++i) {
                array[i].key = _writes[i];
                array[i].table_id = 0;
        }                
}

bool ycsb_update::Run()
{
        uint32_t i, j, num_records, num_updates;
        char *record;
        uint64_t *temp;

        num_records = _writes.size();
        num_updates = 10;        
        for (i = 0; i < num_records; ++i) {
                record = (char*)get_write_ref(_writes[i], 0);
                for (j = 0; j < num_updates; ++j) {
                        temp = (uint64_t*)&record[j*100];
                        *temp += _updates[j];
                }
        }
        return true;
}

ycsb_read_write::ycsb_read_write(vector<uint64_t> reads, 
                                 vector<uint64_t> writes)
{
        uint32_t i, nreads, nwrites;
        
        memset(_accumulated, 0x0, sizeof(uint64_t)*10);
        nreads = reads.size();
        nwrites = writes.size();
        for (i = 0; i < nreads; ++i) 
                _reads.push_back(reads[i]);
        for (i = 0; i < nwrites; ++i)
                _writes.push_back(writes[i]);
}

uint32_t ycsb_read_write::num_reads()
{
        return _reads.size();
}

uint32_t ycsb_read_write::num_rmws()
{
        return _writes.size();
}

void ycsb_read_write::get_reads(big_key *array)
{
        uint32_t i, nreads;

        nreads = _reads.size();
        for (i = 0; i < nreads; ++i) {
                array[i].key = _reads[i];
                array[i].table_id = 0;
        }
}

void ycsb_read_write::get_rmws(big_key *array)
{
        uint32_t i, nwrites; 
        
        nwrites = _writes.size();
        for (i = 0; i < nwrites; ++i) {
                array[i].key = _writes[i];
                array[i].table_id = 0;
        }
}

bool ycsb_read_write::Run()
{
        uint32_t i, nreads, nwrites, total;        
        char *record;

        total = 0;
        nreads = _reads.size();
        for (i = 0; i < nreads; ++i) {
                record = (char*)get_read_ref(_reads[i], 0);
                total += *(uint32_t*)record;
        }

        nwrites = _writes.size();
        for (i = 0; i < nwrites; ++i) {
                record = (char*)get_write_ref(_writes[i], 0);
                *(uint32_t*)record += total;
        }


        return true;
}
