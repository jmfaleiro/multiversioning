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

        lock_list = NULL;
        num_reads = action->readset.size();
        for (i = 0; i < num_reads; ++i) {
                cur_lock = this->lock_allocator->get_lock();
                cur_lock.key = action->readset[i];
                
        }
        
        num_writes = action->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                
        }
}

void lock_table::insert_struct(lock_struct *lock)
{

}

split_action* lock_table::release_locks(split_action *action)
{
        
}
