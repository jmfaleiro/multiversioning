#ifndef SPLIT_ACTION_H_
#define SPLIT_ACTION_H_

#include <machine.h>
#include <vector>
#include <db.h>
#include <graph.h>
#include <action.h>

enum access_t {
        SPLIT_READ,
        SPLIT_WRITE,
};

struct split_key {
        big_key 	_record;	/* Record indexed by this key */
        void 		*_value;		
        split_action 	*_action;
        access_t 	_type;
        split_action 	*_dep;
        split_key 	*_read_dep;
        uint32_t 	_read_count;
};

class split_action;

struct rendezvous_point {
        volatile uint64_t __attribute__((__packed__, __aligned__(CACHE_LINE))) counter;
        split_action *to_run;
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
                UNPROCESSED,
                SCHEDULED,
                EXECUTED,
                COMPLETE,
        };

        friend class split_executor;
        friend class graph_test;
        friend class split_action_queue;
        friend class ready_queue;
        friend class linked_queue;

 private:
        volatile uint64_t 		_state;
        
        /* Data for abortable actions */
        bool 				_can_abort;
        commit_rvp 			*_commit_rendezvous;

        /* Data for upstream nodes  */
        rendezvous_point 		**_rvps;

        /* Data for downstream nodes */
        split_action 			*_rvp_sibling;
        volatile uint64_t 		_dependency_flag;
 
        /* The partition on which this sub-action needs to execute. */
        uint32_t 			_partition_id;

        split_action 			*_left;
        split_action 			*_right;

        uint32_t 			_read_index;
        uint32_t 			_write_index;

 public:

        std::vector<split_key> 		_readset;
        std::vector<split_key> 		_writeset;
        uint32_t 			_rvp_count;
        
        split_action(txn *t, uint32_t partition_id, uint64_t dependency_flag, 
                     bool can_abort);
        uint64_t remote_deps();
        virtual bool run();
        uint32_t get_partition_id();
        
        /* Abstract action state machine */
        split_action::split_action_state get_state();
        void transition_scheduled();
        void transition_executed();
        void transition_complete();
        void transition_complete_remote();

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
        int rand();
};

#endif // SPLIT_ACTION_H_
