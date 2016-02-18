#ifndef LOCAL_LOCK_TABLE_H_
#define LOCAL_LOCK_TABLE_H_

#include <split_action.h>
#include <cpuinfo.h>

struct lock_struct_queue;

enum lock_type {
        READ_LOCK,
        WRITE_LOCK,
};


struct action_queue {
        split_action *head;
        split_action *tail;
};


struct lock_struct {
        struct big_key key;		/* Record corresponding to the lock */
        lock_type type;			/* READ_LOCK or WRITE_LOCK */
        bool is_held;			/* true if the lock is currently held */
        split_action *action;		/* action correspoding to lock */
        lock_struct *left;		/* left link of doubly-linked list */
        lock_struct *right;	        /* right link of doubule-linked list. */
        lock_struct *list_ptr;	        /* list of locks held by an action */
        lock_struct_queue *table_queue;	/* XXX fill this in.*/
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
        void* operator new(std::size_t sz, int cpu) {
                return alloc_mem(sz, cpu);
        }
        
        lock_struct_manager(lock_struct *lst);
        lock_struct* get_lock();
        void return_lock(lock_struct *lock);
};

class lock_table {
 private:
        lock_table_config config;
        lock_struct_manager *lock_allocator;
        lock_struct_queue **tables;
        
        void init_tables();
        void init_allocator();

        static bool conflicting(lock_struct *lock1, lock_struct *lock2);
        static bool check_conflict(big_key key, lock_struct_queue *queue, 
                                   lock_type lck_tp, 
                                   lock_struct ***tail_ptr);
        //        static bool insert_queue(lock_struct *lock, lock_struct_queue *queue);

        lock_struct_queue* get_slot(big_key key);

        void init_tables(lock_table_config config);
        //        bool acquire_single(lock_struct *lock);
        ready_queue release_single(lock_struct *lock);
        void release_multi_partition(lock_struct *lock);
        bool can_wakeup(lock_struct *lock);
        ready_queue get_runnables(lock_struct *lock);
        bool pass_lock(lock_struct *lock);
        lock_struct* find_descendant(lock_struct *lock);


 public:
        void* operator new(std::size_t sz, int cpu) {
                return alloc_mem(sz, cpu);
        }

        lock_table(lock_table_config config);
        void acquire_locks(split_action *action);
        void release_locks(split_action *action, ready_queue *queue);
};

#endif // LOCAL_LOCK_TABLE_H_
