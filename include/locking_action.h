#ifndef LOCKING_ACTION_H_
#define LOCKING_ACTION_H_

#include <db.h>
#include <table.h>
#include <vector>
#include <machine.h>
#include <record_buffer.h>
#include <mcs.h>
#include <insert_buf_mgr.h>
#include <table_mgr.h>
#include <mcs_rw.h>
#include <pipelined_executor.h>

class locking_action;
class locking_worker;
class LockManager;
//namespace pipelined;
//class pipelined::executor;

enum locking_action_status {
        UNEXECUTED,
        COMPLETE,
};

enum locking_key_status {
        PRE_INSERT = 0,
        INSERT_COMPLETE = 1,
};

struct locking_key {

public:
        locking_key(uint64_t key, uint32_t table_id, bool is_write);
        locking_key();
        
        uint32_t table_id;
        uint64_t key;
        bool is_write;
        locking_action *dependency;
        bool is_held;
        volatile uint64_t *latch;
        struct locking_key *prev;
        struct locking_key *next;
        bool is_initialized;
        //        locking_worker *_worker;

        void *value;
        void *buf;
        mcs_rw::mcs_rw_node lock_node;
        void *txn;

        volatile uint64_t dep_status;

        bool operator==(const struct locking_key &other) const
        {
                return this->table_id == other.table_id &&
                this->key == other.key;
        }

        bool operator>(const struct locking_key &other) const
        {
                return (this->table_id > other.table_id) ||
                        (
                         (this->table_id == other.table_id) &&
                         (this->key > other.key)
                         );
        }

        bool operator<(const struct locking_key &other) const
        {
                return (this->table_id < other.table_id) ||
                        (
                         (this->table_id == other.table_id) &&
                         (this->key < other.key)
                         );
        }

        bool operator>=(const struct locking_key &other) const
        {
                return !(*this < other);
        }

        bool operator<=(const struct locking_key &other) const
        {
                return !(*this > other);
        }

        bool operator!=(const struct locking_key &other) const
        {
                return !(*this == other);
        }

        static uint64_t hash(const struct locking_key *k)
        {
                return Hash128to64(std::make_pair(k->key,
                                                  (uint64_t)(k->table_id)));
        }

        uint64_t Hash() const
        {
                return locking_key::hash(this);
        }
};

class lck_key_allocator {
 private:
        locking_key 		*_free_list;
        uint32_t 		_cursor;
        uint32_t 		_sz;
        
 public:
        void* operator new(std::size_t sz, int cpu);        
        lck_key_allocator(uint32_t sz, int cpu);
        locking_key* get();
        void reset();
};

class locking_action : public translator {
        friend class LockManagerTable;
        friend class LockManager;
        friend class locking_worker;
        friend class pipelined::executor;
        
 private:
        locking_action();
        locking_action(const locking_action&);
        locking_action& operator=(const locking_action&);
        
        volatile uint64_t __attribute__((__aligned__(CACHE_LINE)))
                num_dependencies;
        Runnable *worker;
        locking_action *next;
        locking_action *prev;
        lck_key_allocator	*key_alloc;

        table_mgr *tables;
        insert_buf_mgr *insert_mgr;
        bool prepared;
        mcs_struct *lck;
        uint32_t read_index;
        uint32_t write_index;
        bool finished_execution;
        RecordBuffers *bufs;
        //        LockManager *lock_mgr;
        volatile locking_action_status status;
        
        locking_key 		*inserted;
        std::vector<locking_key> writeset;
        std::vector<locking_key> readset;        

        void commit_writes(bool commit);
        void* lookup(locking_key *key);
        
        int find_key(uint64_t key, uint32_t table_id,
                     std::vector<locking_key> key_list);
        
 public:
        locking_action(txn *txn);
        void add_read_key(uint64_t key, uint32_t table_id);
        void add_write_key(uint64_t key, uint32_t table_id);

        void* write_ref(uint64_t key, uint32_t table_id);
        void* read(uint64_t key, uint32_t table_id);
        void* insert_ref(uint64_t key, uint32_t table_id);
        void remove(uint64_t key, uint32_t table_id);
        int rand();
        uint64_t gen_guid();
        void prepare();
        bool Run();
        void finish_inserts();
};

#endif // LOCKING_ACTION_H_
