#ifndef SPLIT_EXECUTOR_H_
#define SPLIT_EXECUTOR_H_

#include <concurrent_queue.h>
#include <split_action.h>
#include <runnable.hh>
#include <cpuinfo.h>
#include <table.h>

#define 	DEP_ARRAY_SZ 		(1<<20)
#define		SPLIT_RECORD_SIZE(sz) 	(sz+sizeof(split_key*))

struct split_record {
        big_key 	key;
        uint64_t 	epoch;
        split_dep 	*key_struct;
        char		*value;
};

typedef SimpleQueue<split_action_batch> splt_inpt_queue;

struct split_executor_config {
        uint32_t 			cpu;
        uint32_t 			num_partitions;
        uint32_t 			partition_id;
        uint32_t 			outstanding_threshold;
        splt_inpt_queue 		*input_queue;
        splt_inpt_queue			*output_queue;
        Table 				**tables;
};

struct split_queue {
        split_action 	*_head;
        split_action    *_tail;
        uint32_t 	_count;
};

class split_executor : public Runnable {
 private:
        struct split_executor_config 		config;
        struct split_queue 			queue;
        
        uint64_t 				dep_index;
        split_dep 				*dep_array;
        uint64_t epoch;
        uint32_t 				num_outstanding;

        /* Manage cross-partition dependencies. */
        void sync_commit_rvp(split_action *action, bool committed);
        void schedule_single_rvp(split_action *exec, rendezvous_point *rvp);
        void schedule_downstream_pieces(split_action *action);
        void commit_remotes(split_action *action);

        /* Manage outstanding unexecuted transactions */
        void add_pending(split_action *action);
        split_action* remove_pending(split_action *action);
        void exec_pending();
        
        /* Execution functions */
        void run_batch(split_action_batch batch);
        bool try_execute(split_action *action);
        bool process_action(split_action *action);
        bool check_ready(split_action *action);

        /* Scheduling functions */
        void schedule_batch(split_action_batch batch);
        void schedule_single(split_action *action);
        void schedule_operation(big_key &key, split_action *action, 
                                access_t type);
        void signal_reads(split_action *action);


        /* Dep array stuff */
        void init_dep_array();
        uint64_t cur_dep();
        split_dep* get_dep();
        void reset_dep();
        void do_check();

 protected:
        virtual void StartWorking();
        virtual void Init();        

 public:
        void* operator new(std::size_t sz, int cpu) {
                return alloc_mem(sz, cpu);
        }
        
        split_executor(struct split_executor_config config);        
};

#endif // SPLIT_EXECUTOR_H_
