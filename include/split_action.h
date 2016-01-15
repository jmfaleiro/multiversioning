#ifndef SPLIT_ACTION_H_
#define SPLIT_ACTION_H_

#include <machine.h>
#include <vector>
#include <db.h>
#include <graph.h>

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
         
        /* 
         * This list is constructed before transactions actually run. Basically,
         * the set of transactions to notify. 
         */        
        split_action **dependents;
        //        split_action *next;
        uint32_t num_dependents;

        uint64_t num_partition_dependencies;
        void* list_ptr;
        volatile uint64_t num_intra_dependencies;

        /* The partition on which this sub-action needs to execute. */
        uint32_t partition_id;
        
 public:
        split_action *exec_list;
        std::vector<big_key> readset;
        std::vector<big_key> writeset;
        
        split_action(txn *t, uint32_t partition_id);
        bool ready();
        virtual bool run();
        virtual void release_multi_partition();
        virtual void remove_partition_dependency();
        uint32_t get_partition_id();
        void add_partition_dependency();
        void set_lock_list(void* list_ptr);
        void* get_lock_list();
        
        /* Rendezvous point functions */
        void set_rvp(rendezvous_point *rvp);
        void set_rvp_wakeups(rendezvous_point **rvps, uint32_t count);

        /* Translator interface functions  */
        void* write_ref(uint64_t key, uint32_t table_id);
        void* read(uint64_t key, uint32_t table_id);
        int rand();
};

#endif // SPLIT_ACTION_H_
