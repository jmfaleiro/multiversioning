#ifndef SPLIT_ACTION_H_
#define SPLIT_ACTION_H_

#include <vector>
#include <db.h>

/*
 * Assumes that split actions are always partition local. 
 */
class split_action {
        friend class split_executor;
 private:

        /* 
         * This list is constructed before transactions actually run. Basically,
         * the set of transactions to notify. 
         */        
        split_action **dependents;
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

        bool ready();
        virtual bool run() = 0;
        virtual void release_multi_partition();
        virtual void remove_partition_dependency();
        uint32_t get_partition_id();
        void add_partition_dependency();
        void set_lock_list(void* list_ptr);
        void* get_lock_list();
};

#endif // SPLIT_ACTION_H_
