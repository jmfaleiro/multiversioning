#include <local_lock_table.h>

lock_struct* lock_struct_manager::get_lock()
{
        lock_struct *ret;
        assert(this->lock_list != NULL);
        ret = this->lock_list;
        this->lock_list = ret->right;
        ret->right = NULL;
        return ret;
}

void lock_struct_manager::return_lock(lock_struct *lock)
{
        lock->left = NULL;
        lock->right = NULL;
        lock->up = NULL;
        lock->down = NULL;
        lock->action = NULL;
        lock->right = this->lock_list;
        this->lock_list = lock;
}

void lock_table::acquire_locks(split_action *action)
{
        uint32_t num_reads, num_writes, i;
        lock_struct *cur_lock, *lock_list;
        bool acquired = false;
        
        lock_list = NULL;
        num_reads = action->readset.size();
        for (i = 0; i < num_reads; ++i) {
                cur_lock = this->lock_allocator->get_lock();
                cur_lock->key = action->readset[i];
                cur_lock->type = READ_LOCK;
                cur_lock->list_ptr = lock_list;
                lock_list = cur_lock;
                if (insert_struct(cur_lock))
                        action->add_partition_dependency();
        }
        
        num_writes = action->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                cur_lock = this->lock_allocator->get_lock();
                cur_lock->key = action->writeset[i];
                cur_lock->type = WRITE_LOCK;
                cur_lock->list_ptr = lock_list;
                lock_list = cur_lock;
                if (insert_struct(cur_lock))
                        action->add_partition_dependency();
        }
        action->set_lock_list(lock_list);
}


split_action* lock_table::release_locks(split_action *action)
{
        action_queue unblocked;
        split_action *ret, *temp;
        lock_struct *locks, *cur;
        
        ret = NULL;
        locks = (lock_struct*)action->get_lock_list();
        while (locks != NULL) {
                cur = locks;
                unblocked = release_single(cur);
                unblocked.tail->exec_list = ret;
                ret = unblocked.head;
                this->lock_allocator->return_lock(cur);
                locks = locks->next;
        }
        return ret;
}

bool lock_table::acquire_single(lock_struct *lock)
{
        return true;
}

action_queue lock_table::release_single(lock_struct *lock)
{
        action_queue ret;

        /* 
         * Two things to do. Release local partition locks, and release global 
         * locks on splits of the same transaction. 
         */
        return ret;
}
