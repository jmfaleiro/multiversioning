#ifndef SPLIT_ACTION_H_
#define SPLIT_ACTION_H_

#include <machine.h>
#include <vector>
#include <db.h>
#include <graph.h>
#include <action.h>

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
        friend class split_executor;
        friend class graph_test;

 private:
        
        /* Data for upstream nodes  */
        rendezvous_point **rvps;
        uint32_t rvp_count;

        /* Data for downstream nodes */
        split_action *rvp_sibling;
        volatile bool dependency_flag;
 
        /* State for local locks */
        uint32_t num_pending_locks;
        void* list_ptr;

        /* The partition on which this sub-action needs to execute. */
        uint32_t partition_id;
        
 public:
        split_action *exec_list;
        std::vector<big_key> readset;
        std::vector<big_key> writeset;
        
        split_action(txn *t, uint32_t partition_id, uint64_t dependency_flag);
        bool ready();
        virtual bool run();
        
        virtual void release_multi_partition();
        uint32_t get_partition_id();
        
        /* Interface used by local lock table */
        void set_lock_list(void* list_ptr);
        void* get_lock_list();
        virtual void decr_pending_locks();
        virtual void incr_pending_locks();
        
        /* Rendezvous point functions */
        void set_rvp(rendezvous_point *rvp);
        void set_rvp_wakeups(rendezvous_point **rvps, uint32_t count);
        split_action* get_rvp_sibling();
        uint32_t num_downstream_rvps();
        rendezvous_point** get_rvps();

        /* Translator interface functions  */
        void* write_ref(uint64_t key, uint32_t table_id);
        void* read(uint64_t key, uint32_t table_id);
        int rand();
};

#endif // SPLIT_ACTION_H_
