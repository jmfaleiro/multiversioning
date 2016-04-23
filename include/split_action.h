#ifndef SPLIT_ACTION_H_
#define SPLIT_ACTION_H_

#include <machine.h>
#include <vector>
#include <db.h>
#include <graph.h>
#include <action.h>
#include <insert_buf_mgr.h>
#include <table.h>

enum access_t {
        SPLIT_READ,
        SPLIT_WRITE,
};

struct split_dep {
        big_key 	_key;
        void 		*_value;		
        split_action 	*_action;
        access_t 	_type;
        split_action 	*_dep;
        split_dep 	*_read_dep;
        uint32_t 	_read_count;
};

class split_action;

struct split_action_batch {
        split_action **actions;
        uint32_t num_actions;
};

struct rendezvous_point {
        volatile uint64_t __attribute__((__packed__, __aligned__(CACHE_LINE))) counter;
        volatile uint64_t __attribute((__packed__, __aligned__(CACHE_LINE))) done;
        bool flattened;
        uint64_t num_actions;
        split_action *to_run;
        split_action **actions;
        split_action *after_txn;
};

enum split_action_status {
        ACTION_UNDECIDED = 0,
        ACTION_COMMITTED = 1,
        ACTION_ABORTED = 2,
};

/* 
 * Rendezvous point which is used to coordinate action commit. In order to 
 * commit, num_committed must equal num_actions. 
 * 
 * state moves from ACTION_UNDECIDED to either ACTION_COMMITTED or 
 * ACTION_ABORTED.
 */
struct commit_rvp {
        volatile uint64_t 	num_committed;
        volatile uint64_t	status;
        uint32_t 		num_actions;
        split_action 		**to_notify;

}__attribute__((__packed__, __aligned__(CACHE_LINE)));

/*
 * Assumes that split actions are always partition local. 
 */
class split_action : public translator {
 public:
        enum split_action_state {
                UNPROCESSED = 0,
                SCHEDULED = 1,
                EXECUTED = 2,
                COMPLETE = 3,
        };

        friend class split_executor;
        friend class graph_test;
        friend class split_action_queue;
        friend class ready_queue;
        friend class linked_queue;
        friend class shared_split_executor;

 private:
        uint64_t 		_state;

        /* For inserts */
        insert_buf_mgr 		*_insert_mgr;
        Table 			**_tables;
        
        /* Data for abortable actions */
        bool 				_can_abort;
        commit_rvp 			*_commit_rendezvous;


        /* Data for upstream nodes  */
        rendezvous_point 		**_rvps;
        bool 				_is_post;

        /* Data for downstream nodes */
        split_action 			*_rvp_sibling;
        rendezvous_point 		*_dep_rvp;
        uint64_t	 		_dependency_flag;
 
        /* The partition on which this sub-action needs to execute. */
        uint32_t 			_partition_id;

        split_action 			*_left;
        split_action 			*_right;
        
        uint32_t 			_dep_index;
        split_dep 			*_dependencies;
        bool 				_outstanding_flag;

 public:

        std::vector<big_key> 		_readset;
        std::vector<big_key> 		_writeset;
        uint32_t 			_rvp_count;
        
        split_action(txn *t, uint32_t partition_id, uint64_t dependency_flag, 
                     bool can_abort, bool is_post);
        
        /* Executor functions */
        uint64_t remote_deps();
        virtual bool run();
        uint32_t get_partition_id();
        split_dep* get_dependencies();
        //        virtual bool check_ready();
        virtual uint32_t* get_dep_index();

        
        /* Abstract action state machine */
        split_action::split_action_state get_state();
        void transition_scheduled();
        void transition_executed();
        void transition_complete();
        void transition_complete_remote();
        bool check_complete();
        bool abortable();
        
        /* Rendezvous point functions */
        void set_rvp(rendezvous_point *rvp);
        void set_rvp_wakeups(rendezvous_point **rvps, uint32_t count);
        split_action* get_rvp_sibling();
        uint32_t num_downstream_rvps();
        rendezvous_point** get_rvps();
        void clear_dependency_flag();
        void set_commit_rvp(commit_rvp *rvp);
        commit_rvp *get_commit_rvp();
        
        /* Translator interface functions  */
        void* write_ref(uint64_t key, uint32_t table_id);
        void* read(uint64_t key, uint32_t table_id);
        void *insert_ref(uint64_t key, uint32_t table_id);
        void remove(uint64_t key, uint32_t table_id);
        int rand();
        uint64_t gen_guid();
};

#endif // SPLIT_ACTION_H_
