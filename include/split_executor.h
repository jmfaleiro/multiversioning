#ifndef SPLIT_EXECUTOR_H_
#define SPLIT_EXECUTOR_H_

#include <concurrent_queue.h>
#include <split_action.h>
#include <runnable.hh>
#include <cpuinfo.h>
#include <table.h>

#define		SPLIT_RECORD_SIZE(sz) 	(sz+sizeof(split_key*))

struct split_record {
        split_key 	*key_struct;
        char		*value;
};

struct split_action_batch {
        split_action **actions;
        uint32_t num_actions;
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

        /* Manage cross-partition dependencies. */
        void sync_commit_rvp(split_action *action, bool committed);
        void schedule_single_rvp(rendezvous_point *rvp);        
        void schedule_downstream_pieces(split_action *action);

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
        void schedule_operation(split_key *key);
        
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
