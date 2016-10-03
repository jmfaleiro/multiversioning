#ifndef OCC_ACTION_H_
#define OCC_ACTION_H_

#include <action.h>
#include <table.h>
#include <db.h>
#include <record_buffer.h>
#include <mcs.h>
#include <insert_buf_mgr.h>
#include <table_mgr.h>

#define TIMESTAMP_MASK (0xFFFFFFFFFFFFFF00)
#define EPOCH_MASK (0xFFFFFFFF00000000)

#define GET_LOCK_HOLDER(tid) (tid & ~(TIMESTAMP_MASK))
#define CREATE_TID(epoch, counter) ((((uint64_t)epoch)<<32) | (((uint64_t)counter)<<4))
#define GET_TIMESTAMP(tid) ((tid) & TIMESTAMP_MASK)
#define GET_EPOCH(tid) ((tid & EPOCH_MASK)>>32)
#define GET_COUNTER(tid) (GET_TIMESTAMP(tid) & ~EPOCH_MASK)
#define IS_LOCKED(tid) ((tid & ~(TIMESTAMP_MASK)) != 0)
#define RECORD_TID_PTR(rec_ptr) ((volatile uint64_t*)rec_ptr)
#define RECORD_VALUE_PTR(rec_ptr) ((void*)&(((uint64_t*)rec_ptr)[1]))
#define OCC_RECORD_SIZE(value_sz) (sizeof(uint64_t)+value_sz)
#define REAL_RECORD_SIZE(value_sz) (value_sz - sizeof(uint64_t))

enum validation_err_t {
        READ_ERR,
        VALIDATION_ERR,
        INSERT_ERR,
};

class OCCWorker;

class occ_validation_exception : public std::exception {
 public:
        occ_validation_exception(validation_err_t err) { this->err = err; }
        validation_err_t err;
};



struct occ_txn_status {
        bool validation_pass;
        bool commit;
};


class occ_composite_key {
 public:
        uint32_t tableId;
        uint64_t key;
        uint64_t old_tid;
        bool is_rmw;
        bool is_locked;
        bool is_initialized;
        RecordBuffy *buffer;
        void *lock;
        void *record_ptr;

        occ_composite_key(uint32_t tableId, uint64_t key, bool is_rmw);
        void* GetValue() const ;
        uint64_t GetTimestamp();
        bool ValidateRead();

        void* StartRead();
        bool FinishRead();
        
        bool operator==(const occ_composite_key &other) const {
                return other.tableId == this->tableId && other.key == this->key;
        }

        bool operator!=(const occ_composite_key &other) const {
                return !(*this == other);
        }
  
        bool operator<(const occ_composite_key &other) const {
                return ((this->tableId < other.tableId) || 
                        ((this->tableId == other.tableId) && (this->key < other.key)));
        }
  
        bool operator>(const occ_composite_key &other) const {
                return ((this->tableId > other.tableId) ||
                        ((this->tableId == other.tableId) && (this->key > other.key)));
        }
  
        bool operator<=(const occ_composite_key &other) const {
                return !(*this > other);
        }
  
        bool operator>=(const occ_composite_key &other) const {
                return !(*this < other);
        }
};


class OCCAction : public translator {
        friend class OCCWorker;

 private:
        OCCAction();
        OCCAction& operator=(const OCCAction&);
        OCCAction(const OCCAction&);

        RecordBuffers *record_alloc;
        Table **tables;
        Table **lock_tables;
        mcs_mgr *mgr;
        uint64_t tid;
        OCCWorker *worker;
        table_mgr *tbl_mgr;
        insert_buf_mgr *insert_mgr;
        mcs_struct *lck;

        conc_table_record *inserted;
        uint32_t insert_ptr;
        uint32_t cpu_id;
        std::vector<occ_composite_key> inserts;
        std::vector<occ_composite_key> readset;
        std::vector<occ_composite_key> writeset;
        std::vector<occ_composite_key> shadow_writeset;

        virtual uint64_t stable_copy(uint64_t key, uint32_t table_id,
                                     void **rec_ptr, void *record_copy); 
        virtual bool validate_single(void *value, uint64_t read_tid, 
                                     bool is_rmw);

        virtual void cleanup_single(occ_composite_key &comp_key);
        virtual void install_single_write(void *record_ptr, void *value, 
                                          mcs_struct *lck_ptr,
                                          size_t record_sz);
        virtual void install_single_insert(occ_composite_key &comp_key);

        inline bool try_acquire_single(volatile uint64_t *lock_ptr);
        inline void acquire_single(volatile uint64_t *lock_ptr);
        inline void release_single(volatile uint64_t *lock_word);
        
 public:
        
        OCCAction(txn *txn);
        virtual void create_inserts(uint32_t n_inserts);
        OCCAction *link;
        
        virtual void *write_ref(uint64_t key, uint32_t table);
        virtual void *read(uint64_t key, uint32_t table);
        virtual void* insert_ref(uint64_t key, uint32_t table);
        virtual void remove(uint64_t key, uint32_t table);
        virtual int rand();
        virtual uint64_t gen_guid();

        virtual void set_allocator(RecordBuffers *buf);
        virtual void set_tables(Table **tables, Table **lock_tables);
        virtual void set_mgr(mcs_mgr *mgr);

        virtual bool run();
        virtual void acquire_locks();
        virtual void check_locks();
        virtual bool validate();
        virtual uint64_t compute_tid(uint32_t epoch, uint64_t last_tid);
        virtual void install_writes();
        virtual void release_locks();
        virtual void cleanup();
        virtual void undo_inserts();
        
        void add_read_key(uint32_t table_id, uint64_t key);
        void add_write_key(uint32_t table_id, uint64_t key, bool is_rmw);
}; 

#endif // OCC_ACTION_H_
