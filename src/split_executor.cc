#include <split_executor.h>

void add_action(action_queue *queue, split_action *action);

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

void split_executor::schedule_action(split_action *action)
{
        uint32_t pid;
        pid = action->partition_id;
        ready_queues[pid]->EnqueueBlocking(action);
}

/*
 * Schedule dependent actions that need to run.
 */
void split_executor::schedule_intra_deps(split_action *action)
{
        uint32_t i;
        split_action *dep;

        for (i = 0; i < action->num_dependents; ++i) {
                dep = action->dependents[i];
                if (fetch_and_decrement(dep->num_intra_dependencies) == 0)
                        schedule_action(dep);
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
        schedule_intra_deps(action);
        descendants = this->lock_table->release_locks(action);
        while (descendants != NULL) {
                run_action(descendants);
                descendants = descendants->next;
        }
}

/*
 * 
 */
void split_executor::check_pending()
{
        uint32_t num_partitions, i;
        split_action *action;
        action_queue queue;
        
        /* Collect actions whose remote dependencies are satisfied. */
        queue.head = NULL;
        queue.tail = NULL;
        for (i = 0; i < num_partitions; ++i) {
                while (ready_queue[i]->Dequeue(&action)) 
                        if (action->ready())
                                add_action(&queue, action);
        }
        
        /* Run actions that are ready to execute. */
        action = queue.head;
        while (action != NULL) {
                run_action(action);
                action = action->exec_list;
        }
}

void split_executor::Init()
{
}

/*
 * Executor threads's "main" function.
 */
void split_executor::StartWorking()
{
        split_action_batch batch;
        split_action *action;
        uint32_t num_partitions, i, j;

        while (true) {
                batch = input_queue->DequeueBlocking();
                for (i = 0; i < batch.num_actions; ++i) {
                        process_action(batch.actions[i]);
                        check_pending();
                }
        }
}
