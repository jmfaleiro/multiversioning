#ifndef SPLIT_EXECUTOR_H_
#define SPLIT_EXECUTOR_H_

#include <concurrent_queue.h>
#include <split_action.h>
#include <local_lock_table.h>
#include <runnable.hh>
#include <cpuinfo.h>

struct split_action_batch {
        split_action **actions;
        uint32_t num_actions;
};

typedef SimpleQueue<split_action_batch> splt_inpt_queue;
typedef SimpleQueue<split_action*> splt_comm_queue;

struct split_executor_config {
        uint32_t cpu;
        uint32_t num_partitions;
        uint32_t partition_id;
        SimpleQueue<split_action*> **ready_queues;
        SimpleQueue<split_action*> **signal_queues;
        SimpleQueue<split_action_batch> *input_queue;
        SimpleQueue<split_action_batch> *output_queue;
        struct lock_table_config lock_table_conf;
};

class split_executor : public Runnable {
 private:
        struct split_executor_config config;
        lock_table *lck_table;
        splt_inpt_queue *input_queue;
        splt_inpt_queue *output_queue;
        splt_comm_queue **ready_queues;
        splt_comm_queue **signal_queues;

        void schedule_single_rvp(rendezvous_point *rvp);        
        void schedule_downstream_pieces(split_action *action);
        void run_action(split_action *action);
        void process_action(split_action *action);
        void schedule_action(split_action *action);
        void check_pending();
        
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
