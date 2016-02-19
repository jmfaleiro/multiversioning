#include <local_lock_table.h>
#include <cassert>
#include <cpuinfo.h>

void merge_queues(action_queue *merge_into, action_queue *queue);


inline static bool queue_invariant(lock_struct_queue *queue)
{

        return ((queue->head == NULL && queue->tail == NULL) ||
                (queue->head != NULL && queue->tail != NULL)) &&
                (queue->head == NULL || queue->head->is_held == true);
}

lock_table::lock_table(lock_table_config config)
{
        this->config = config;
        std::cerr << config.cpu << "\n";
        init_tables();
        init_allocator();
}

/*
 * For each database table, allocate memory to store its corresponding lock 
 * table. 
 */
void lock_table::init_tables()
{
        uint32_t i;
        size_t alloc_sz;

        alloc_sz = sizeof(struct lock_struct_queue*)*config.num_tables;
        tables = (struct lock_struct_queue**) alloc_mem(alloc_sz, 
                                                        (int)config.cpu);
        memset(tables, 0x0, alloc_sz);
        for (i = 0; i < config.num_tables; ++i) {
                alloc_sz = 
                        sizeof(struct lock_struct_queue)*config.table_sizes[i];
                tables[i] = (struct lock_struct_queue*) alloc_mem(alloc_sz, 
                                                                  (int)config.cpu);
                memset(tables[i], 0x0, alloc_sz);
        }
}

void lock_table::init_allocator()
{
        lock_struct *lck_lst;
        size_t alloc_sz;
        uint32_t i;

        alloc_sz = sizeof(lock_struct)*config.num_lock_structs;
        lck_lst = (lock_struct*)alloc_mem(alloc_sz, (int)config.cpu);
        memset(lck_lst, 0x0, alloc_sz);
        for (i = 0; i < config.num_lock_structs; ++i) 
                lck_lst[i].right = &lck_lst[i+1];        
        lck_lst[i-1].right = NULL;
        lock_allocator = new((int)config.cpu) lock_struct_manager(lck_lst);
}

lock_struct_manager::lock_struct_manager(lock_struct *lst)
{
        this->lock_list = lst;
}

/*
 * Add a single piece to a lock table queue.
 */
/*
void add_action(action_queue *queue, split_action *action)
{
        bool ready;
        
        ready = action->ready();
        assert(action->exec_list == NULL);
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
*/

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
        uint32_t num_reads, num_writes, i, conflicts;
        lock_struct *cur_lock, *lock_list;
        lock_struct **ptrs[action->readset.size() + action->writeset.size()];
        lock_struct_queue *queues[action->readset.size() + action->writeset.size()];

        action->set_lock_flag();
        action->set_shortcut_flag();

        conflicts = 0;
        num_reads = action->readset.size();
        for (i = 0; i < num_reads; ++i) {
                queues[i] = get_slot(action->readset[i]._record);
                if (!check_conflict(action->readset[i]._record, queues[i], 
                                    READ_LOCK,
                                    &ptrs[i])) {
                        conflicts += 1;
                        action->incr_pending_locks();
                }
        }        
        
        num_writes = action->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                queues[num_reads+i] = get_slot(action->writeset[i]._record);
                if (!check_conflict(action->writeset[i]._record, queues[num_reads+i], 
                                    WRITE_LOCK,
                                    &ptrs[i+num_reads])) {
                        conflicts += 1;
                        action->incr_pending_locks();
                }
        }
        
        if (action->abortable() == false && conflicts == 0 && 
            action->remote_deps() == false) {
                action->transition_locked();
                return;
        }

        action->reset_shortcut_flag();        
        lock_list = NULL;

        /* Insert read locks */
        for (i = 0; i < num_reads; ++i) {
                cur_lock = this->lock_allocator->get_lock();
                cur_lock->key = action->readset[i]._record;
                cur_lock->table_queue = NULL;
                cur_lock->type = READ_LOCK;
                cur_lock->action = action;
                cur_lock->list_ptr = lock_list;
                lock_list = cur_lock;
                cur_lock->is_held = !(*ptrs[i] != NULL && 
                                      (*ptrs[i])->type == WRITE_LOCK);
                queues[i]->add_lock(cur_lock);
        }

        /* Insert write locks */
        for (i = 0; i < num_writes; ++i) {
                cur_lock = this->lock_allocator->get_lock();
                cur_lock->key = action->writeset[i]._record;
                cur_lock->table_queue = NULL;
                cur_lock->type = WRITE_LOCK;
                cur_lock->list_ptr = lock_list;
                cur_lock->action = action;
                lock_list = cur_lock;
                cur_lock->is_held = (*ptrs[num_reads+i] == NULL);
                queues[num_reads+i]->add_lock(cur_lock);
        }
        action->set_lock_list(lock_list);
        action->transition_locked();
}

/* 
 * Called after action has finished executing. Use this to schedule the 
 * execution of succeeding actions.
 */
void lock_table::release_locks(split_action *action, ready_queue *queue)
{
        assert(queue->is_empty() == true);

        ready_queue unblocked;
        lock_struct *locks, *cur;
        
        if (action->shortcut_flag() == false) {
                locks = (lock_struct*)action->get_lock_list();
                while (locks != NULL) {
                        assert(locks->action == action);
                        cur = locks;
                        unblocked = release_single(cur);
                        ready_queue::merge_queues(queue, &unblocked);
                        locks = locks->list_ptr;
                        this->lock_allocator->return_lock(cur);
                }
        }
        action->transition_complete();
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
bool lock_table::check_conflict(big_key key, lock_struct_queue *queue, 
                                lock_type lck_tp, lock_struct ***tail_ptr)
{
        bool acquired = false;
        *tail_ptr = &queue->tail;

        /* Find the first ancestor in the queue. */
        while (**tail_ptr != NULL && (**tail_ptr)->key != key)
                *tail_ptr = &((**tail_ptr)->left);
        
        /* No ancestor, ie, no prior logical locks. */
        if (**tail_ptr == NULL) 
                acquired = true;
        /* Ancestor's logical lock conflicts. */
        else if (lck_tp == WRITE_LOCK || (**tail_ptr)->type == WRITE_LOCK)
                acquired = false;
        /* Ancestor holds logical lock and both are reads. */
        else if ((**tail_ptr)->is_held == true) 
                acquired = true;
        else 
                acquired = false;

        return acquired;        
}

lock_struct_queue* lock_table::get_slot(big_key key)
{
        uint64_t index, num_slots;
        
        num_slots = config.table_sizes[key.table_id];
        index = big_key::Hash(&key) % num_slots;
        return &tables[key.table_id][index];
}


/*

bool lock_table::insert_queue(lock_struct *lock, lock_struct_queue *queue)
{
        bool acquired;

        acquired = lock_table::check_conflict(lock, queue);        
        lock->left = NULL;
        lock->right = NULL;
        assert((acquired == false && lock->is_held == false) || 
               (acquired == true && lock->is_held == true));

        if (queue->head == NULL) { 
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

  
        table_id = lock->key.table_id;
        num_slots = this->config.table_sizes[table_id];
        index = big_key::Hash(&lock->key) % num_slots;
        acquired = insert_queue(lock, &this->tables[table_id][index]);
        return acquired;
}
*/

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
        assert(action->get_state() == split_action::LOCKED);
        lock->is_held = true;
        action->decr_pending_locks();
        return action->ready();
}

lock_struct* lock_table::find_descendant(lock_struct *lock)
{
        lock_struct *desc = NULL;
        
        if (can_wakeup(lock)) {
                desc = lock->right;
                while (desc != NULL) {
                        if (desc->key == lock->key)
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
ready_queue lock_table::get_runnables(lock_struct *lock)
{
        lock_struct *descendant;
        ready_queue ret;

        if (!can_wakeup(lock)) 
                return ret;        
        descendant = find_descendant(lock);
        if (descendant != NULL) {
                if (descendant->type == WRITE_LOCK) {
                        if (pass_lock(descendant)) 
                                ret.enqueue(descendant->action);
                } else {	
                        assert(false);
                        /* Descendant is a reader. Unblock a chain of readers.*/
                        assert(descendant->type == READ_LOCK);
                        while (descendant != NULL && 
                               descendant->type == READ_LOCK) {
                                if (pass_lock(descendant)) 
                                        ret.enqueue(descendant->action);
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
        lock->table_queue = this;

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
ready_queue lock_table::release_single(lock_struct *lock)
{
        ready_queue ret;

        assert(lock->is_held == true);
        ret = get_runnables(lock);
        lock->action->release_multi_partition();
        lock->table_queue->remove_lock(lock);
        return ret;
}
