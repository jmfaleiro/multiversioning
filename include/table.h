#ifndef                 TABLE_H_
#define                 TABLE_H_

#include <city.h>
#include <cpuinfo.h>
#include <cassert>

extern bool split_flag;

struct TableRecord {
        struct TableRecord *next;
        struct TableRecord *next_in;
        uint64_t key;
        uint32_t table_id;
        uint32_t state;
        volatile uint64_t ref_count;
        char *value;
};

struct TableConfig {
  uint64_t tableId;
  uint64_t numBuckets;
  int startCpu;
  int endCpu;
  uint64_t freeListSz;
  uint64_t valueSz;
        uint32_t recordSize;
};

class Table {
 private:
  Table() {
  }

  TableRecord **buckets;
  TableRecord *freeList;
  TableConfig  conf;
  bool init;
  TableRecord *default_value;
  
  inline TableRecord* GetRecord() {
    assert(freeList != NULL);
    TableRecord *ret = freeList;
    freeList = freeList->next;
    ret->next = NULL;
    return ret;
  }

 public:
  void* operator new(std::size_t sz, int cpu) {
          return alloc_mem(sz, cpu);
  }

  void SetInit() {
    this->init = true;
  }


  static Table* copy_table(Table *tbl, int cpu)
  {
          Table *ret = new (cpu) Table();
          ret->buckets = tbl->buckets;
          ret->freeList = tbl->freeList;
          ret->conf = tbl->conf;
          ret->init = tbl->init;
          ret->default_value = tbl->default_value;
          return ret;
  }

  Table(TableConfig conf) {
          
    this->init = false;
    this->conf = conf;    
    
    // Initialize hash buckets
    if (split_flag) {
            buckets = (TableRecord**)alloc_mem((conf.numBuckets+1)*sizeof(TableRecord*), 
                                               conf.startCpu);
    } else {
            buckets = (TableRecord**)alloc_interleaved_all((conf.numBuckets+1)*sizeof(TableRecord*));

    }

    memset(buckets, 0x0, conf.numBuckets*sizeof(TableRecord*));

    // Initialize freelist
    if (conf.freeListSz > 0) {
            uint32_t recordSz = sizeof(TableRecord)+conf.valueSz;
            char *data;
            TableRecord *records;
            if (split_flag) {
                    records = (TableRecord*)alloc_mem(conf.freeListSz*sizeof(TableRecord), conf.startCpu);
                    assert(records != NULL);
                    data = (char*)alloc_mem(conf.freeListSz*conf.valueSz, conf.startCpu);
                    assert(data != NULL);
            } else {
                    records = (TableRecord*)alloc_interleaved_all(conf.freeListSz*sizeof(TableRecord));
                    assert(records != NULL);
                    data = (char*)alloc_interleaved_all(conf.freeListSz*conf.valueSz);
                    assert(data != NULL);
            }

            memset(records, 0x0, conf.freeListSz*sizeof(TableRecord));
            memset(data, 0x0, conf.freeListSz*conf.valueSz);
            uint64_t i;
            for (i = 0; i < conf.freeListSz; ++i) {
                    records[i].next = &records[i+1];
                    records[i].value = (data + i*conf.valueSz);
            }    
            records[i-1].next = NULL;
            freeList = records;
            //    default_value = GetRecord();
            //    memset(default_value->value, 0x0, conf.valueSz);
    }
  }

  virtual void PutEmpty(uint64_t key)
  {
    uint64_t index = 
      Hash128to64(std::make_pair(conf.tableId, key)) % conf.numBuckets;
    TableRecord *rec = GetRecord();
    rec->next = buckets[index];
    rec->key = key;
    buckets[index] = rec;
  }
  
  virtual void Put(uint64_t key, void *value)
  {
    uint64_t index = 
      Hash128to64(std::make_pair(conf.tableId, key)) % conf.numBuckets;
    TableRecord *rec = GetRecord();
    rec->next = buckets[index];
    rec->key = key;
    memcpy(rec->value, value, conf.valueSz);
    buckets[index] = rec;
  }


  virtual void PutGiven(uint64_t key, TableRecord *rec)
  {
    uint64_t index = 
      Hash128to64(std::make_pair(conf.tableId, key)) % conf.numBuckets;
    rec->next = buckets[index];
    rec->key = key;
    buckets[index] = rec;
          
  }

  virtual void Insert(uint64_t key, TableRecord *rec)
  {
          uint64_t index; 
          index = Hash128to64(std::make_pair(conf.tableId, key)) % conf.numBuckets;
          rec->next = buckets[index];
          buckets[index] = rec;
  }
  
  virtual void* Get(uint64_t key) {
          uint64_t index = 
                  Hash128to64(std::make_pair(conf.tableId, key)) % conf.numBuckets;
          TableRecord *rec = buckets[index];
          while (rec != NULL && rec->key != key) {
                  rec = rec->next;
          }
          assert(rec != NULL);
          return (void*)(rec->value);
  }

  virtual void* GetAlways(uint64_t key) {
          if (this->init == true)
                  return Get(key);
          
          uint64_t index = 
                  Hash128to64(std::make_pair(conf.tableId, key)) % conf.numBuckets;
          TableRecord *rec = buckets[index];
          while (rec != NULL && rec->key != key) {
                  rec = rec->next;
          }
          if (rec != NULL) {
                  return (void*)(rec->value);
          } else {
                  PutEmpty(key);
                  return Get(key);
          }
  }
  
  uint32_t RecordSize()
  {
          return conf.valueSz;
  }
};

#endif          // TABLE_H_
