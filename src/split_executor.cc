#include <split_executor.h>

split_executor::split_executor(struct split_executor_config config)
        : Runnable((int)config.cpu)
{
        this->config = config;
        queue._head = NULL;
        queue._tail = NULL;
        queue._count = 0;
}

void split_executor::sync_commit_rvp(split_action *action, bool committed)
{
        commit_rvp *rvp;
        bool sched_rights;
        uint32_t i;
        split_action **to_notify;
        uint64_t prev_state;

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
                for (i = 0; i < rvp->num_actions; ++i) 
                        to_notify[i]->transition_complete_remote();
        }
}

void split_executor::schedule_single_rvp(rendezvous_point *rvp)
{
        split_action *action;

        if (fetch_and_decrement(&rvp->counter) > 0) 
                return;
        
        action = rvp->to_run;
        while (action != NULL) {
                action->clear_dependency_flag();
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

void split_executor::Init()
{
}

/*
 * Executor threads's "main" function.
 */
void split_executor::StartWorking()
{
        split_action_batch batch;
        
        while (true) {
                batch = config.input_queue->DequeueBlocking();
                schedule_batch(batch);
                run_batch(batch);
                config.output_queue->EnqueueBlocking(batch);
        }
}

void split_executor::schedule_operation(split_key *key)
{
        split_record *entry;
        split_key *prev;

        /* XXX Intialize the entry some how */
        entry = (split_record*)config.tables[key->_record.table_id]->Get(key->_record.key);

        prev = entry->key_struct;
        if (prev != NULL)
                assert(prev->_value == entry->value);
        key->_value = entry->value;
        key->_read_count = 1;
        key->_read_dep = key;

        if (prev == NULL) {
                key->_dep = NULL;
                entry->key_struct = key;
        } else if (key->_type == SPLIT_WRITE) {
                key->_dep = prev->_action;
                key->_read_count = 0;
                entry->key_struct = key;
        } else if (key->_type == SPLIT_READ && prev->_type == SPLIT_READ) {
                key->_dep = prev->_dep;
                prev->_read_count += 1;
                key->_read_dep = prev;
        } else if (prev->_type == SPLIT_WRITE) {
                assert(key->_type == SPLIT_READ);
                key->_dep = prev->_action;
                key->_read_count = 1;
                key->_read_dep = key;
                entry->key_struct = key;
        } else {
                assert(false);
        }
}

void split_executor::schedule_single(split_action *action)
{
        uint32_t nreads, nwrites, i;
        split_key *key;

        assert(action->get_state() == split_action::UNPROCESSED);
        action->transition_scheduled();
        nreads = action->_readset.size();
        for (i = 0; i < nreads; ++i) {
                key = &action->_readset[i];
                schedule_operation(key);
        }
        
        nwrites = action->_writeset.size();
        for (i = 0; i < nwrites; ++i) {
                key = &action->_writeset[i];
                schedule_operation(key);
        }
}

void split_executor::schedule_batch(split_action_batch batch)
{
        uint32_t i;
        
        for (i = 0; i < batch.num_actions; ++i)
                schedule_single(batch.actions[i]);
}

bool split_executor::check_ready(split_action *action)
{
        uint32_t i , num_reads, num_writes, *read_index, *write_index;
        bool ready;        
        split_action *dep;

        ready = true;
        read_index = &action->_read_index;
        num_reads = action->_readset.size();
        for (; *read_index < num_reads; *read_index += 1) {
                i = *read_index;
                dep = action->_readset[i]._dep;
                if (dep != NULL && 
                    dep->get_state() != split_action::COMPLETE && 
                    process_action(dep) == false) {
                        ready = false;
                        break;
                }                
        }

        write_index = &action->_write_index;
        num_writes = action->_writeset.size();
        for (; *write_index < num_writes; *write_index += 1) {
                i = *write_index;
                dep = action->_writeset[i]._dep;
                if (dep != NULL &&
                    dep->get_state() != split_action::COMPLETE &&
                    process_action(dep) == false) {
                        ready = false;
                        break;
                }
        }
        return ready;
}

bool split_executor::process_action(split_action *action)
{
        assert(action != NULL);
        volatile uint64_t state;
        barrier();
        state = action->get_state();
        barrier();
        
        if (state != split_action::COMPLETE) {
                if (state == split_action::SCHEDULED &&
                    action->remote_deps() == 0) {
                        if (try_execute(action) == true) {
                                return true;
                        } else {
                                /* An ancestor hasn't yet finished */
                                return false;
                        }
                } else {
                        /* 
                         * Waiting for a remote dep to begin executin, or 
                         * a remote ack to commit 
                         */
                        return false;
                }
        } else {
                /* state == split_action::COMPLETE */                
                return true;
        }
}

bool split_executor::try_execute(split_action *action) 
{
        bool commit;

        if (check_ready(action)) {
                commit = action->run();
                if (action->abortable() == true) 
                        sync_commit_rvp(action, commit);
                else {
                        schedule_downstream_pieces(action);
                        action->transition_complete();
                }
                return true;
        } else {
                return false;
        }
}


void split_executor::run_batch(split_action_batch batch)
{
        uint32_t i;
        split_action *action;

        for (i = 0; i < batch.num_actions; ++i) {
                action = batch.actions[i];
                if (!process_action(action)) {
                        add_pending(action);
                }
                
                while (queue._count > config.outstanding_threshold) 
                        exec_pending();
        }
        
        while (queue._count > 0) 
                exec_pending();

}

void split_executor::add_pending(split_action *action)
{
        assert((queue._head == NULL && queue._tail == NULL) || 
               (queue._head != NULL && queue._tail != NULL));
        assert((queue._head != NULL && queue._count > 0) || 
               (queue._head == NULL && queue._count == 0));

        action->_left = NULL;
        action->_right = NULL;
        if (queue._head == NULL) {
                queue._head = action;
        } else {
                queue._tail->_right = action;
                action->_left = queue._tail;
        }
        
        queue._count += 1;
        queue._tail = action;
                
}

split_action* split_executor::remove_pending(split_action *action)
{
        assert((queue._head == NULL && queue._tail == NULL) || 
               (queue._head != NULL && queue._tail != NULL));
        assert((queue._head != NULL && queue._count > 0) || 
               (queue._head == NULL && queue._count == 0));
        
        split_action *ret;
        ret = action->_right;

        if (queue._head == action) {
                queue._head = action->_right;
        } else {
                action->_left->_right = action->_right;
        }
        
        if (queue._tail == action) {
                queue._tail = action->_left;
        } else {
                action->_right->_left = action->_left;
        }
        
        queue._count -= 1;        
        return ret;
}

void split_executor::exec_pending()
{
        split_action *cur;
        cur = queue._head;
        
        while (cur != NULL) {                
                if (process_action(cur))
                        cur = remove_pending(cur);
                else 
                        cur = cur->_right;
        }       
}
