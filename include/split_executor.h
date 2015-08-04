#ifndef SPLIT_EXECUTOR_H_
#define SPLIT_EXECUTOR_H_

#include <concurrent_queue.h>
#include <split_action.h>
#include <local_lock_table.h>

struct split_action_batch {
        split_action **actions;
        uint32_t num_actions;
};

struct split_executor_config {
        uint32_t cpu;
        uint32_t num_partitions;
        uint32_t partition_id;
        SimpleQueue<split_action*> ready_queues;
        SimpleQueue<split_action_batch> *input_queue;
        struct lock_table_config lock_table_conf;
};

class split_executor : public Runnable {
 private:
        struct split_executor_config config;

        SimpleQueue<split_action_batch> *input_queue;
        SimpleQueue<split_action*> **ready_queues;
        
        void run_action(split_action *action);
        void process_action(split_action *action);
        void schedule_intra_deps(split_action *action);
        void schedule_action(split_action *action);
        
 protected:
        virtual void StartWorking();
        virtual void Init();        

 public:
        void* operator new(std::size_t sz, int cpu);
        split_executor(struct split_executor_config config);        
};

#endif // SPLIT_EXECUTOR_H_
