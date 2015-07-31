#include <split_executor.h>

/*
 * XXX If we find that an action is immediately ready to run, it seems wasteful 
 * to acquire locks, run, and then release. Why acquire locks in the first 
 * place? We could use a "bloom filter", that tells us about locks that have 
 * been acquired. If the transaction does not conflict with other locks in the 
 * bloom filter, then we can run without acquiring or releasing locks.
 */
void split_executor::process_action(split_action *action)
{
        this->lock_table->acquire_locks(action);
        if (action->ready()) {
                action->run();
                this->lock_table->release_locks(action);
        }
}

/*
 * Run an action, and (recursively) every other action that is unblocked by the 
 * root's execution.
 */
void split_executor::run_action(split_action *action)
{
        bool is_ready;
        split_action *descendants;
        
        is_ready = action->ready();
        assert(is_ready == true);
        action->run();
        descendants = this->lock_table->release_locks(action);
        while (descendants != NULL) {
                run_action(descendants);
                descendants = descendants->next;
        }
}

