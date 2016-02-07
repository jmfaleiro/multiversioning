#include <split_executor.h>

void add_action(action_queue *queue, split_action *action);

void merge_queues(action_queue *merge_into, action_queue *queue)
{
        if (queue->head == NULL) {
                assert(queue->tail == NULL);
                return;
        }

        if (merge_into->head == NULL) {
                assert(merge_into->tail == NULL);
                merge_into->head = queue->head;
                merge_into->tail = queue->tail;
        } else {
                assert(merge_into->tail != NULL);
                assert(merge_into->tail->exec_list == NULL);
                assert(queue->tail->exec_list == NULL);
                merge_into->tail->exec_list = queue->head;
                merge_into->tail = queue->tail;
        }
}

split_executor::split_executor(struct split_executor_config config)
        : Runnable((int)config.cpu)
{
        this->config = config;
        this->lck_table = 
                new((int)config.cpu) lock_table(config.lock_table_conf);
        this->tables = config.tables;
        this->input_queue = config.input_queue;
        this->output_queue = config.output_queue;
        this->ready_queues = config.ready_queues;
        this->signal_queues = config.signal_queues;
}

void split_executor::run_action(split_action *action, action_queue *queue)
{
        assert(action->ready());
        //        assert(action->state != split_action::COMPLETE);
        queue->head = NULL;
        queue->tail = NULL;
        action->tables = tables;
        action->run();
        num_pending -= 1;
        schedule_downstream_pieces(action);
        if (action->shortcut_flag() == false)
                lck_table->release_locks(action, queue);
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
        assert(action->get_state() == split_action::UNPROCESSED);

        action_queue descendants;

        descendants.head = NULL;
        descendants.tail = NULL;
        this->lck_table->acquire_locks(action);
        if (action->ready()) 
                run_action(action, &descendants);
        assert(descendants.head == NULL);
}

void split_executor::schedule_single_rvp(rendezvous_point *rvp)
{
        split_action *action;
        uint32_t partition;
        assert(false);
        if (fetch_and_decrement(&rvp->counter) > 0) 
                return;

        action = rvp->to_run;
        assert(action != NULL);
        while (action != NULL) {
                partition = action->get_partition_id();
                action->clear_dependency_flag();
                signal_queues[partition]->EnqueueBlocking(action);
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
/*
void split_executor::process_pending(split_action *action, action_queue *descendants)
{
        bool is_ready;
        
        is_ready = action->ready();
        assert(is_ready == true);
        run_action(action, descendants);
}
*/

action_queue split_executor::exec_list(split_action *action_list)
{
        assert(action_list != NULL);
        action_queue to_exec, temp;

        to_exec.head = NULL;
        to_exec.tail = NULL;
        while (action_list != NULL) {
                run_action(action_list, &temp);
                merge_queues(&to_exec, &temp);
                action_list = action_list->exec_list;
        }
        
        return to_exec;
}

/*
 * 
 */
split_action* split_executor::check_pending()
{
        uint32_t i;
        split_action *action;
        action_queue queue;
        
        /* Collect actions whose remote dependencies are satisfied. */
        queue.head = NULL;
        queue.tail = NULL;
        for (i = 0; i < config.num_partitions; ++i) {
                while (ready_queues[i]->Dequeue(&action)) {
                        if (action->get_state() == split_action::UNPROCESSED && action->ready()) {
                                add_action(&queue, action);
                        }
                }
        }
        return queue.head;
}

void split_executor::Init()
{
}

void split_executor::do_pending() 
{
        action_queue temp;
        split_action *action;
        
        action = check_pending();
        if (action != NULL) 
                while (true) {
                        temp = exec_list(action);
                        if (temp.head != NULL)
                                action = temp.head;
                        else 
                                break;
                }

}


/*
 * Executor threads's "main" function.
 */
void split_executor::StartWorking()
{
        split_action_batch batch;
        split_action *action;
        uint32_t i;
        action_queue temp;
        
        while (true) {
                batch = input_queue->DequeueBlocking();
                num_pending = batch.num_actions;
                for (i = 0; i < batch.num_actions; ++i) {
                        process_action(batch.actions[i]);
                        assert(batch.actions[i]->done_locking);
                        //                        check_pending();
                }
                while (num_pending != 0) {                                                
                        action = check_pending();
                        i = 0;
                        if (action != NULL) 
                                while (true) {
                                        temp = exec_list(action);
                                        if (temp.head != NULL)
                                                action = temp.head;
                                        else 
                                                break;
                                }
                }
                output_queue->EnqueueBlocking(batch);
        }
}
