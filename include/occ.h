#ifndef 	OCC_H_
#define		OCC_H_

#include <runnable.hh>
#include <table.h>
#include <concurrent_queue.h>
#include <occ_action.h>
#include <exception>
#include <record_buffer.h>
#include <table_mgr.h>
#include <tpcc.h>

#define NUM_MCS_LOCKS	1000

extern size_t *tpcc_record_sizes;

struct OCCActionBatch {
        uint32_t batchSize;
        OCCAction **batch;
};

struct occ_log_header {
        uint32_t table_id;
        uint64_t key;
        uint64_t tid;
        uint32_t record_len;
};

struct OCCWorkerConfig {
        SimpleQueue<OCCActionBatch> *inputQueue;
        SimpleQueue<OCCActionBatch> *outputQueue;
        int cpu;
        table_mgr 	*tbl_mgr;
        Table **tables;
        Table **lock_tables;
        bool is_leader;
        volatile uint32_t *epoch_ptr;
        volatile uint64_t num_completed;
        uint64_t epoch_threshold;
        uint64_t log_size;
        bool globalTimestamps;
        uint32_t num_tables;
};

struct district_wrapper {
        uint64_t tid;
        district_record district;
};

struct warehouse_wrapper {
        uint64_t tid;
        warehouse_record warehouse;
};

class OCCWorker : public Runnable {
 public:
        static table_mgr **mgr_array;

 private:        
        OCCWorkerConfig 	config;
        uint64_t 		incr_timestamp;
        uint64_t 		last_tid;
        uint32_t 		last_epoch;
        uint32_t 		txn_counter;
        RecordBuffers 		*bufs;
        mcs_mgr 		*mgr;
        insert_buf_mgr		*insert_mgr;

        district_wrapper 	district[10];
        warehouse_wrapper 	warehouse;
        
        virtual bool RunSingle(OCCAction *action);
        virtual uint32_t exec_pending(OCCAction **action_list);
        virtual void UpdateEpoch();
        virtual void EpochManager();
        virtual void TxnRunner();
        
 protected:
        virtual void StartWorking();
        virtual void Init();
 public:
        void* operator new(std::size_t sz, int cpu)
        {
                return alloc_mem(sz, cpu);
        }
        
        OCCWorker(OCCWorkerConfig conf, RecordBuffersConfig rb_conf);
        virtual uint64_t NumCompleted();
        
        district_wrapper *get_district(uint32_t d);
        warehouse_wrapper *get_warehouse();
};

#endif		// OCC_H_
