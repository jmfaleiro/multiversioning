#ifndef SPLIT_EXECUTOR_H_
#define SPLIT_EXECUTOR_H_

#include <concurrent_queue.h>
#include <split_action.h>
#include <local_lock_table.h>
#include <runnable.hh>
#include <cpuinfo.h>
#include <table.h>

struct split_action_batch {
        split_action **actions;
        uint32_t num_actions;
};

enum message_t {
        READY,
        EXECUTED,
};

struct split_message {
        uint32_t 	partition;
        message_t 	type;        
        split_action 	*action;
};

typedef SimpleQueue<split_action_batch> splt_inpt_queue;
typedef SimpleQueue<split_message> splt_comm_queue;

struct split_executor_config {
        uint32_t cpu;
        uint32_t num_partitions;
        uint32_t partition_id;
        uint32_t outstanding_threshold;
        SimpleQueue<split_message> *single_ready_queue;
        SimpleQueue<split_message> **ready_queues;
        SimpleQueue<split_message> **signal_queues;
        SimpleQueue<split_action_batch> *input_queue;
        SimpleQueue<split_action_batch> *output_queue;
        struct lock_table_config lock_table_conf;
        Table **tables;
};

class split_executor : public Runnable {
 private:
        struct split_executor_config config;
        lock_table *lck_table;
        Table **tables;
        splt_inpt_queue *input_queue;
        splt_inpt_queue *output_queue;
        splt_comm_queue *single_ready_queue;
        splt_comm_queue **ready_queues;
        splt_comm_queue **signal_queues;
        uint32_t num_pending;
        uint32_t num_outstanding;

        uint32_t pending_run_count;

        void schedule_single_rvp(rendezvous_point *rvp);        
        void schedule_downstream_pieces(split_action *action);
        void run_action(split_action *action, ready_queue *queue);

        /* Functions related to inter-partition communication */
        void exec_pending();
        ready_queue check_pending();
        ready_queue exec_list(ready_queue ready);
        void get_new_messages(linked_queue *release_queue, 
                              linked_queue *ready_queue);
        ready_queue process_release_msgs(linked_queue release_queue);
        ready_queue process_ready_msgs(ready_queue to_exec, linked_queue msgs);

        void commit_action(split_action *action);
        void process_action(split_action *action);
        void schedule_action(split_action *action);

        void sync_commit_rvp(split_action *action, bool committed, 
                             ready_queue *queue);
        
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
