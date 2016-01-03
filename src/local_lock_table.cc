#include <local_lock_table.h>
#include <cassert>
#include <cpuinfo.h>

inline static bool queue_invariant(lock_struct_queue *queue)
{
        return (queue->head == NULL && queue->tail == NULL) ||
                (queue->head != NULL && queue->tail != NULL);
}

/*
 * Add a single piece to a lock table queue.
 */
void add_action(action_queue *queue, split_action *action)
{
        bool ready;
        
        ready = action->ready();
        assert(queue != NULL && action != NULL);
        assert((queue->head == NULL && queue->tail == NULL) ||
               (queue->head != NULL && queue->head != NULL));
        assert(ready == true);        
        if (queue->head == NULL) 
                queue->head = action;
        else 
                queue->tail->exec_list = action;
        queue->tail = action;
}

/*
 * Get a single lock struct.
 */
lock_struct* lock_struct_manager::get_lock()
{
        lock_struct *ret;
        assert(this->lock_list != NULL);
        ret = this->lock_list;
        this->lock_list = ret->right;
        ret->right = NULL;
        return ret;
}

/*
 * Return a single lock struct.
 */
void lock_struct_manager::return_lock(lock_struct *lock)
{
        lock->left = NULL;
        lock->action = NULL;
        lock->right = this->lock_list;
        this->lock_list = lock;
}

/* 
 * Acquire the locks associated with a particular split action.
 */
void lock_table::acquire_locks(split_action *action)
{
        uint32_t num_reads, num_writes, i;
        lock_struct *cur_lock, *lock_list;
        
        lock_list = NULL;
        num_reads = action->readset.size();
        for (i = 0; i < num_reads; ++i) {
                cur_lock = this->lock_allocator->get_lock();
                cur_lock->key = action->readset[i];
                cur_lock->type = READ_LOCK;
                cur_lock->list_ptr = lock_list;
                lock_list = cur_lock;
                if (!acquire_single(cur_lock))
                        action->add_partition_dependency();
        }
        
        num_writes = action->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                cur_lock = this->lock_allocator->get_lock();
                cur_lock->key = action->writeset[i];
                cur_lock->type = WRITE_LOCK;
                cur_lock->list_ptr = lock_list;
                lock_list = cur_lock;
                if (!acquire_single(cur_lock))
                        action->add_partition_dependency();
        }
        action->set_lock_list(lock_list);
}

/* 
 * Called after action has finished executing. Use this to schedule the 
 * execution of succeeding actions.
 */
split_action* lock_table::release_locks(split_action *action)
{
        action_queue unblocked;
        split_action *ret;
        lock_struct *locks, *cur;
        
        ret = NULL;
        locks = (lock_struct*)action->get_lock_list();
        while (locks != NULL) {
                cur = locks;
                unblocked = release_single(cur);
                unblocked.tail->exec_list = ret;
                ret = unblocked.head;
                this->lock_allocator->return_lock(cur);
                locks = locks->list_ptr;
        }
        return ret;
}

/*
 * 
 */
void lock_table::init_tables(lock_table_config config)
{
        uint32_t i;
        uint64_t total_sz;
        void *data;

        this->tables = 
                (lock_struct_queue**)alloc_mem(sizeof(lock_struct_queue*)*
                                               config.num_tables, 
                                               config.cpu);
        for (i = 0; i < config.num_tables; ++i) {
                total_sz = sizeof(lock_struct_queue)*config.table_sizes[i];
                data = alloc_mem(total_sz, config.cpu);
                memset(data, 0x0, total_sz);
                this->tables[i] = (lock_struct_queue*)data;
        }        
}

/*
 * Two logical locks conflict if any one is a write-lock.
 */
bool lock_table::conflicting(lock_struct *lock1, lock_struct *lock2)
{
        assert(lock1->key == lock2->key);
        return lock1->type == WRITE_LOCK || lock2->type == WRITE_LOCK;
}

/* 
 * Check if "lock" conflicts with any prior locks in the lock queue. 
 * Returns true if lock is acquired.
 */
bool lock_table::check_conflict(lock_struct *lock, lock_struct_queue *queue)
{
        lock_struct *ancestor;
        bool acquired = false;
        ancestor = queue->tail;

        /* Find the first ancestor in the queue. */
        while (ancestor != NULL && ancestor->key != lock->key)
                ancestor = ancestor->left;

        /* No ancestor, ie, no prior logical locks. */
        if (ancestor == NULL) 
                acquired = true;
        /* Ancestor's logical lock conflicts. */
        else if (lock_table::conflicting(ancestor, lock) == true) 
                acquired = false;
        /* Ancestor holds logical lock and both are reads. */
        else if (ancestor->is_held == true) 
                acquired = true;
        else 
                acquired = false;
        lock->is_held = acquired;
        return !acquired;        
}

bool lock_table::insert_queue(lock_struct *lock, lock_struct_queue *queue)
{
        bool acquired;

        acquired = lock_table::check_conflict(lock, queue);        
        lock->left = NULL;
        lock->right = NULL;
        assert((acquired == false && lock->is_held == false) || 
               (acquired == true && lock->is_held == true));

        if (queue->head == NULL) { /* The queue is empty. */                
                assert(queue->tail == NULL);
                queue->head = lock;
                queue->tail = lock;
        } else {
                assert(queue->tail != NULL);
                queue->tail->right = lock;
                lock->left = queue->tail;
                queue->tail = lock;
        }
        return acquired;
}

bool lock_table::acquire_single(lock_struct *lock)
{
        uint64_t index, num_slots;        
        uint32_t table_id;
        bool acquired;

        /* Find the slot in which to insert the lock. */
        table_id = lock->key.table_id;
        num_slots = this->config.table_sizes[table_id];
        index = big_key::Hash(&lock->key) % num_slots;
        acquired = insert_queue(lock, &this->tables[table_id][index]);
        return acquired;
}

/*
 * XXX This function may need to be optimized.
 * Check whether releasing the lock struct allows later locks to be released.
 */ 
bool lock_table::can_wakeup(lock_struct *lock)
{
        lock_struct *prev;

        assert(lock != NULL && lock->is_held == true);
        if (lock->type == READ_LOCK) {
                prev = lock->left;
                while (prev != NULL && prev->key != lock->key)
                        prev = prev->left;
                if (prev != NULL) {
                        assert(prev->type == READ_LOCK && 
                               prev->is_held == true);
                        return false;
                }
        }
        return true;
}

/*
 * Pass a logical lock to the given argument.
 */
bool lock_table::pass_lock(lock_struct *lock)
{
        split_action *action;

        action = lock->action;
        assert(lock != NULL);
        assert(lock->is_held == false);
        assert(action != NULL);
        lock->is_held = true;
        action->remove_partition_dependency();
        return action->ready();
}

lock_struct* lock_table::find_descendant(lock_struct *lock)
{
        lock_struct *desc = NULL;
        
        if (can_wakeup(lock)) {
                desc = lock->right;
                while (desc != NULL) {
                        if (desc->key == lock->key && 
                            desc->action->ready() == true)
                                break;
                        desc = desc->right;
                }
        }
        return desc;
}

/*
 * Find the set of descendant actions that are ready to run as a consequence of 
 * releasing the current lock. 
 */
action_queue lock_table::get_runnables(lock_struct *lock)
{
        lock_struct *descendant;
        action_queue ret;

        ret.head = NULL;
        ret.tail = NULL;
        if (!can_wakeup(lock)) 
                return ret;        
        descendant = find_descendant(lock);
        if (descendant != NULL) {
                if (descendant->type == WRITE_LOCK) {
                        if (pass_lock(descendant)) 
                                add_action(&ret, descendant->action);
                } else {	
                        /* Descendant is a reader. Unblock a chain of readers.*/
                        assert(descendant->type == READ_LOCK);
                        while (descendant != NULL && 
                               descendant->type == READ_LOCK) {
                                if (pass_lock(descendant)) 
                                        add_action(&ret, descendant->action);
                                descendant = find_descendant(descendant);
                        }
                }
        }
        return ret;
}

void lock_struct_queue::add_lock(lock_struct *lock) 
{
        /* Queue invariant. */
        assert(lock->table_queue == NULL);
        assert(queue_invariant(this) == true);

        if (this->head == NULL) /* Queue is empty. */
                this->head = lock;
        else 
                this->tail->right = lock;        
        lock->left = this->tail;
        lock->right = NULL;
        this->tail = lock;        

        assert(queue_invariant(this) == true);
}

void lock_struct_queue::remove_lock(lock_struct *lock)
{
        /* Queue invariant. */
        assert(lock->table_queue == this);
        assert(queue_invariant(this) == true);
        
        /* Lock is at the head of the queue. */
        if (lock->left == NULL) {
                assert(this->head == lock);
                this->head = lock->right;
        } else {
                lock->left->right = lock->right;
        }

        /* Lock is at the tail of the queue. */
        if (lock->right == NULL) {
                assert(this->tail == lock);
                this->tail = lock->left;
        } else {
                lock->right->left = lock->left;
        }
        
        /* Queue invariant. */
        assert(queue_invariant(this) == true);
}

/*
 * Release a partition local lock. Returns a list of actions that are ready to 
 * run as a result of lock release. 
 */
action_queue lock_table::release_single(lock_struct *lock)
{
        action_queue ret;

        assert(lock->is_held == true);
        ret = get_runnables(lock);
        lock->action->release_multi_partition();
        lock->table_queue->remove_lock(lock);
        return ret;
}
