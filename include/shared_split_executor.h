#ifndef 	SHARED_SPLIT_EXECUTOR_H_
#define 	SHARED_SPLIT_EXECUTOR_H_

#include <runnable.hh>
#include <table.h>
#include <split_action.h>

class shared_split_executor : public Runnable {
 private:
        
        uint32_t exec_id;
        uint32_t num_execs;

        bool process_action(split_action *action);
        bool check_ready(split_action *action);
        bool try_exec(split_action *action);

        void exec_batch(split_action_batch batch);
        void sync_commit_rvp(split_action *action);
};

#endif 		// SHARED_SPLIT_EXECUTOR_H_
