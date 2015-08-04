#ifndef LOCAL_LOCK_TABLE_H_
#define LOCAL_LOCK_TABLE_H_

#include <split_action.h>

enum lock_type {
        READ_LOCK,
        WRITE_LOCK,
};

struct action_queue {
        split_action *head;
        split_action *tail;
};


struct lock_struct {
        struct big_key key;
        lock_type type;
        bool is_held;
        split_action *action;
        lock_struct *left;
        lock_struct *right;
        lock_struct *list_ptr;
        lock_struct_queue *table_queue;
};

struct lock_struct_queue {
        lock_struct *head;
        lock_struct *tail;
        
        void add_lock(lock_struct *lock);
        void remove_lock(lock_struct *lock);
};

struct lock_table_config {
        uint32_t cpu;
        uint32_t num_tables;
        uint64_t *table_sizes;
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
        lock_table_config config;
        lock_struct_manager *lock_allocator;
        lock_struct_queue **tables;
        
        static bool conflicting(lock_struct *lock1, lock_struct *lock2);
        static bool check_conflict(lock_struct *lock, lock_struct_queue *queue);
        static bool insert_queue(lock_struct *lock, lock_struct_queue *queue);

        void init_tables(lock_table_config config);
        bool acquire_single(lock_struct *lock);
        action_queue release_single(lock_struct *lock);
        void release_multi_partition(lock_struct *lock);

 public:
        void acquire_locks(split_action *action);
        split_action* release_locks(split_action *action);
};

#endif // LOCAL_LOCK_TABLE_H_
