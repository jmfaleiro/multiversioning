#ifndef SPLIT_ACTION_H_
#define SPLIT_ACTION_H_

#include <machine.h>
#include <vector>
#include <db.h>
#include <graph.h>
#include <action.h>
#include <table.h>

struct split_key {
        big_key _record;	/* Record indexed by this key */
        void *_value;		
};

class split_txn : public txn {
 public:
        virtual txn_graph* convert_to_graph();
};

class split_action;

struct rendezvous_point {
        volatile uint64_t __attribute__((__packed__, __aligned__(CACHE_LINE))) counter;
        split_action *to_run;
};

/*
 * Assumes that split actions are always partition local. 
 */
class split_action : public translator {
 public:
        enum split_action_state {
                UNPROCESSED,
                PROCESSING,
                COMPLETE,
        };

        friend class split_executor;
        friend class graph_test;


 private:
        split_action::split_action_state state;
        
        /* Data for upstream nodes  */
        rendezvous_point **rvps;

        /* Data for downstream nodes */
        split_action *rvp_sibling;
        volatile bool dependency_flag;
 
        /* State for local locks */
        uint32_t num_pending_locks;
        void* list_ptr;
        bool done_locking;

        /* The partition on which this sub-action needs to execute. */
        uint32_t partition_id;
        
        Table **tables;
        bool shortcut;

 public:

        split_action *exec_list;
        std::vector<split_key> readset;
        std::vector<split_key> writeset;
        uint32_t rvp_count;
        
        split_action(txn *t, uint32_t partition_id, uint64_t dependency_flag);
        bool ready();
        bool remote_deps();
        virtual bool run();
        split_action::split_action_state get_state();
        virtual void release_multi_partition();
        uint32_t get_partition_id();
        
        /* Interface used by local lock table */
        void set_lock_list(void* list_ptr);
        void* get_lock_list();
        virtual void decr_pending_locks();
        virtual void incr_pending_locks();
        void set_lock_flag();
        bool shortcut_flag();
        void set_shortcut_flag();
        void reset_shortcut_flag();
        
        /* Rendezvous point functions */
        void set_rvp(rendezvous_point *rvp);
        void set_rvp_wakeups(rendezvous_point **rvps, uint32_t count);
        split_action* get_rvp_sibling();
        uint32_t num_downstream_rvps();
        rendezvous_point** get_rvps();
        void clear_dependency_flag();

        /* Translator interface functions  */
        void* write_ref(uint64_t key, uint32_t table_id);
        void* read(uint64_t key, uint32_t table_id);
        int rand();
};

#endif // SPLIT_ACTION_H_
