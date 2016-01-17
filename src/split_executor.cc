#include <split_executor.h>

void add_action(action_queue *queue, split_action *action);

split_executor::split_executor(struct split_executor_config config)
        : Runnable((int)config.cpu)
{
        this->config = config;
        this->lck_table = 
                new((int)config.cpu) lock_table(config.lock_table_conf);
        this->input_queue = config.input_queue;
        this->output_queue = config.output_queue;
        this->ready_queues = config.ready_queues;
}

/*
 * XXX If we find that an action is immediately ready to run, it seems wasteful 
 * to acquire locks, run, and then release. Why acquire locks in the first 
 * place? We could use a "bloom filter", that tells us about locks that have 
 * been acquired. If the transaction does not conflict with other locks in the 
 * bloom filter, then we can run without acquiring or releasing locks.
 */
void split_executor::process_action(split_action *action)
{
        this->lck_table->acquire_locks(action);
        assert(action->ready());
        if (action->ready()) {
                action->run();
                this->lck_table->release_locks(action);
        }
}

void split_executor::schedule_single_rvp(rendezvous_point *rvp)
{
        split_action *action;
        uint32_t partition;

        if (fetch_and_decrement(&rvp->counter) > 0)
                return;
        action = rvp->to_run;
        assert(action != NULL);
        while (action != NULL) {
                partition = action->get_partition_id();
                ready_queues[partition]->EnqueueBlocking(action);
                action = action->get_rvp_sibling();
        }
}

void split_executor::schedule_downstream_pieces(split_action *action)
{
        uint32_t i, num_rvps;
        rendezvous_point **rvps;
         
        num_rvps = action->num_downstream_rvps();
        rvps = action->get_rvps();
        for (i = 0; i < num_rvps; ++i) 
                schedule_single_rvp(rvps[i]);
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
        schedule_downstream_pieces(action);
        descendants = this->lck_table->release_locks(action);
        while (descendants != NULL) {
                run_action(descendants);
                descendants = descendants->exec_list;
        }
}

/*
 * 
 */
void split_executor::check_pending()
{
        uint32_t i;
        split_action *action;
        action_queue queue;
        
        /* Collect actions whose remote dependencies are satisfied. */
        queue.head = NULL;
        queue.tail = NULL;
        for (i = 0; i < config.num_partitions; ++i) {
                while (ready_queues[i]->Dequeue(&action)) 
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
        uint32_t i;

        while (true) {
                batch = input_queue->DequeueBlocking();
                for (i = 0; i < batch.num_actions; ++i) {
                        process_action(batch.actions[i]);
                        check_pending();
                }
                output_queue->EnqueueBlocking(batch);
        }
}
