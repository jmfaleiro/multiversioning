#ifndef LOCAL_LOCK_TABLE_H_
#define LOCAL_LOCK_TABLE_H_

#include <split_action.h>

enum lock_type {
        READ_LOCK,
        WRITE_LOCK,
};

struct lock_struct {
        struct big_key key;
        lock_type type;
        split_action *action;
        lock_struct *left;
        lock_struct *right;
        lock_struct *up;
        lock_struct *down;
        lock_struct *list_ptr;
};

class lock_table_config {
        uint32_t num_tables;
        uin64_t *table_sizes;
        uint64_t num_lock_structs;
};

class lock_struct_manager {
 private:
        lock_struct *lock_list;
        
 public:
        lock_struct* get_lock();
        void return_lock(lock_struct *lock);
};

class lock_table {
 private:
        lock_struct_manager *lock_allocator;
        
 public:
        void acquire_locks(split_action *action);
        split_action* release_locks(split_action *action);
};

#endif // LOCAL_LOCK_TABLE_H_
