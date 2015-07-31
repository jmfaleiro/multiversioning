#ifndef SPLIT_EXECUTOR_H_
#define SPLIT_EXECUTOR_H_

#include <concurrent_queue.h>
#include <split_action.h>

struct split_action_batch {
        split_action **actions;
        uint32_t num_actions;
};

class split_executor {
 private:
        split_action_batch *input_queue;

 public:
        void process_action(split_action *action);
};

#endif // SPLIT_EXECUTOR_H_
