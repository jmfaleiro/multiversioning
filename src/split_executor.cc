#include <split_executor.h>

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
        this->single_ready_queue = config.single_ready_queue;
        this->pending_run_count = 0;
}

/*
 * Run an action that is ready to execute.
 */
void split_executor::run_action(split_action *action, ready_queue *queue)
{
        assert(queue->is_empty() == true);
        num_outstanding -= 1;
                
        action->tables = tables;
        action->run();

        schedule_downstream_pieces(action);
        if (action->abortable() == true) {
                sync_commit_rvp(action, true, queue);
        } else {
                num_pending -= 1;
                lck_table->release_locks(action, queue);        
        }
}

void split_executor::sync_commit_rvp(split_action *action, bool committed, 
                                     ready_queue *queue)
{
        commit_rvp *rvp;
        uint64_t prev_state;
        bool sched_rights;
        linked_queue local_actions;
        uint32_t i, partition;
        split_message msg;
        //        split_action *cur;
        split_action **to_notify;

        action->transition_executed();

        /* 
         * First determine whether the current action is responsible for 
         * notifying other partitions. Needed for idempotence.
         */
        sched_rights = false;
        rvp = action->get_commit_rvp();
        if (committed == false) {
                if (cmp_and_swap(&rvp->status, (uint64_t)ACTION_UNDECIDED, 
                                 (uint64_t)ACTION_ABORTED)) 
                        sched_rights = true;         
        } else if (fetch_and_increment(&rvp->num_committed) == rvp->num_actions) {
                sched_rights = true;
                prev_state = xchgq(&rvp->status, (uint64_t)ACTION_COMMITTED);
                assert(prev_state == ACTION_UNDECIDED);
        }


        if (sched_rights) {
                to_notify = rvp->to_notify;
                for (i = 0; i < rvp->num_actions; ++i) {
                        partition = to_notify[i]->get_partition_id();
                        if (partition == config.partition_id) {
                                local_actions.enqueue(to_notify[i]);
                        } else {
                                msg.type = EXECUTED;
                                msg.action = to_notify[i];
                                msg.partition = partition;
                                signal_queues[partition]->EnqueueBlocking(msg);
                        }
                }

                /* For now, can support only a single local abortable action */
                local_actions.seal();
                num_pending -= 1;
                lck_table->release_locks(action, queue);
        }
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

        ready_queue descendants;

        this->lck_table->acquire_locks(action);
        if (action->ready()) { 
                action->transition_scheduled();
                run_action(action, &descendants);
        }
        assert(descendants.is_empty() == true);
}

void split_executor::schedule_single_rvp(rendezvous_point *rvp)
{
        split_message msg;

        if (fetch_and_decrement(&rvp->counter) > 0) 
                return;

        msg.type = READY;
        msg.action = rvp->to_run;
        assert(msg.action != NULL);
        while (msg.action != NULL) {
                msg.partition = msg.action->get_partition_id();
                msg.action->clear_dependency_flag();
                signal_queues[msg.partition]->EnqueueBlocking(msg);
                msg.action = msg.action->get_rvp_sibling();
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
 * Executes all actions that are unblocked due to incoming ready/release 
 * messages. 
 */
void split_executor::exec_pending()
{
        ready_queue action_queue;
        
        action_queue = check_pending();
        if (action_queue.is_empty() == false) {
                while (true) {
                        action_queue = exec_list(action_queue);
                        if (action_queue.is_empty() == true)
                                break;
                }
        }
}

/*
 * Execute a list of actions. Returns a list of actions that are ready to run as
 * a consequence of being unblocked by an input action.
 */
ready_queue split_executor::exec_list(ready_queue ready)
{
        assert(ready.is_empty() == false);
        ready_queue to_exec, temp;
        split_action *action;

        while ((action = ready.dequeue()) != NULL) {
                pending_run_count += 1;
                temp.reset();
                run_action(action, &temp);
                ready_queue::merge_queues(&to_exec, &temp);                
        }
        to_exec.seal();
        return to_exec;
}

/*
 * Inspects messages from other partitions, and determines which transactions 
 * are ready to execute as a consequence. 
 */
ready_queue split_executor::check_pending()
{
        linked_queue released, ready;
        ready_queue to_exec;

        get_new_messages(&released, &ready);
        to_exec = process_release_msgs(released);
        to_exec = process_ready_msgs(to_exec, ready);
        to_exec.seal();
        return to_exec;
}

/* 
 * Returns messages from message-queues. Distinguishes between "release" 
 * messages, corresponding to commit/abort decisions, and "ready" messages whose
 * dependencies have been satisfied. 
 */
void split_executor::get_new_messages(linked_queue *release_queue, 
                                      linked_queue *ready_queue)
{
        /* The queues have to be empty at this point. */
        assert(release_queue->is_empty() == true && 
               ready_queue->is_empty() == true);

        split_message msg;
        uint64_t i, nmsgs;
        bool success;

        nmsgs = single_ready_queue->diff();
        for (i = 0; i < nmsgs; ++i) {
                success = single_ready_queue->Dequeue(&msg);
                assert(success);
                assert(msg.partition == config.partition_id);
                if (msg.type == EXECUTED) {
                        assert(msg.action->get_state() == split_action::EXECUTED);
                        release_queue->enqueue(msg.action);
                }
                else if (msg.type == READY)
                        ready_queue->enqueue(msg.action);
                else
                        assert(false);
        }
        
        /*
        for (i = 0; i < config.num_partitions; ++i) {
                if (ready_queues[i]->Dequeue(&msg)) {
                        if (msg.type == EXECUTED) 
                                release_queue->enqueue(msg.action);
                        else if (msg.type == READY) 
                                ready_queue->enqueue(msg.action);
                        else 
                                assert(false);                        
                }
        }
        */

        release_queue->seal();
        ready_queue->seal();
}


/*
 * 
 */
ready_queue split_executor::process_ready_msgs(ready_queue to_exec, 
                                               linked_queue msgs)
{
        split_action *action;
        
        while ((action = msgs.dequeue()) != NULL) 
                if (action->ready()) 
                        to_exec.enqueue(action);        
        return to_exec;
}


ready_queue split_executor::process_release_msgs(linked_queue release_queue)
{
        ready_queue accumulated, temp;
        split_action *cur;
        
        while ((cur = release_queue.dequeue()) != NULL) {

                /* Action be waited on */
                assert(cur->abortable() == true);
                
                /* Uncommitted actions's writes are buffered */
                if (cur->can_commit() == true)
                        commit_action(cur);

                temp.reset();
                num_pending -= 1;
                lck_table->release_locks(cur, &temp);
                ready_queue::merge_queues(&accumulated, &temp);
        }
        return accumulated;
}

void split_executor::commit_action(__attribute__((unused)) split_action *action)
{
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
        
        ready_queue to_exec;
        
        while (true) {
                //                while (input_queue->Dequeue(&batch) == false)
                //                        exec_pending();
                batch = input_queue->DequeueBlocking();
                num_pending = batch.num_actions;
                num_outstanding = 0;

                for (i = 0; i < batch.num_actions; ++i) {
                        num_outstanding += 1;
                        process_action(batch.actions[i]);      
                        assert(batch.actions[i]->done_locking);
                        while (num_outstanding > config.outstanding_threshold)
                                exec_pending();
                }
                while (num_pending != 0) 
                        exec_pending();
                output_queue->EnqueueBlocking(batch);
        }
}
