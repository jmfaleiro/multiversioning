#include <split_executor.h>

split_executor::split_executor(struct split_executor_config config)
        : Runnable((int)config.cpu)
{
        this->config = config;
        queue._head = NULL;
        queue._tail = NULL;
        queue._count = 0;
        init_dep_array();
        epoch = 1;
}

void split_executor::init_dep_array()
{
        dep_array = (split_dep*)alloc_mem(DEP_ARRAY_SZ*sizeof(split_dep), config.partition_id);
        memset(dep_array, 0x0, DEP_ARRAY_SZ*sizeof(split_dep));
        dep_index = 0;
}

uint64_t split_executor::cur_dep()
{
        return dep_index;
}

split_dep* split_executor::get_dep()
{
        split_dep *ret;
        
        assert(dep_index < DEP_ARRAY_SZ);
        ret = &dep_array[dep_index];
        dep_index += 1;
        return ret;
}

void split_executor::reset_dep()
{
        dep_index = 0;
}

void split_executor::commit_remotes(split_action *action)
{
        assert(action->_can_abort == true);
        commit_rvp *rvp;
        uint64_t prev;

        rvp = action->get_commit_rvp();
        prev = xchgq(&rvp->status, ACTION_COMMITTED);
        assert(prev == ACTION_UNDECIDED);
}

void split_executor::sync_commit_rvp(split_action *action, __attribute__((unused)) bool committed)
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
        to_notify = rvp->to_notify;
        if (committed == false) {
                if (cmp_and_swap(&rvp->status, (uint64_t)ACTION_UNDECIDED, 
                                 (uint64_t)ACTION_ABORTED))
                        sched_rights = true;         
        } else if (fetch_and_increment(&rvp->num_committed) == rvp->num_actions) {
                sched_rights = true;
                prev_state = xchgq(&rvp->status, (uint64_t)ACTION_COMMITTED);
                assert(prev_state == ACTION_UNDECIDED);
        }
        
        if (sched_rights == true) {
                for (i = 0; i < rvp->num_actions; ++i) 
                        to_notify[i]->transition_complete_remote();
        }

        /*
        sched_rights = true;
        rvp = action->get_commit_rvp();
        to_notify = rvp->to_notify;
        barrier();
        for (i = 0; i < rvp->num_actions; ++i) {
                prev_state = to_notify[i]->_state;
                if (prev_state != split_action::EXECUTED) {
                        sched_rights = false;
                        break;
                }
        }
        barrier();
        if (sched_rights) {
                to_notify = rvp->to_notify;
                for (i = 0; i < rvp->num_actions; ++i) {
                        prev_state = xchgq(&to_notify[i]->_state, (uint64_t)split_action::COMPLETE);
                        if (prev_state != split_action::EXECUTED)
                                break;
                }
        }
        */
}

void split_executor::schedule_single_rvp(split_action *exec, 
                                         rendezvous_point *rvp)
{
        //        split_action *action;
        if (fetch_and_decrement(&rvp->counter) == 0) {
                if (exec->abortable() == true)
                        commit_remotes(exec);
                if (rvp->after_txn != NULL)
                        rvp->after_txn->run();
                fetch_and_increment(&rvp->done);
                /*
                action = rvp->to_run;
                while (action != NULL) {
                        action->clear_dependency_flag();
                        action = action->get_rvp_sibling();
                }
                */
        }
}

void split_executor::schedule_downstream_pieces(split_action *action)
{
        uint32_t i, num_rvps;
        rendezvous_point **rvps;

        num_rvps = action->num_downstream_rvps();
        rvps = action->get_rvps();
        /* HACK TO GET COMMITS WORKING PROPERLY. */
        assert(num_rvps <= 1);
        for (i = 0; i < num_rvps; ++i) 
                schedule_single_rvp(action, rvps[i]);
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
                num_outstanding = 0;
                run_batch(batch);
                assert(num_outstanding == 0);
                config.output_queue->EnqueueBlocking(batch);
        }
}

void split_executor::schedule_operation(big_key &key, split_action *action, 
                                        access_t type)
{
        split_record *entry;
        split_dep *prev, *cur;


        /* XXX Intialize the entry some how */
        entry = (split_record*)config.tables[key.table_id]->Get(key.key);
        assert(entry->key == key);
        prev = entry->key_struct;
        if (entry->epoch == epoch && prev != NULL) {
                assert(prev->_key == key);
                assert(prev->_value == entry->value);                
        }

        cur = get_dep();
        cur->_key = key;
        cur->_value = entry->value;
        cur->_action = action;
        cur->_type = type;
        cur->_read_count = 1;
        cur->_read_dep = cur;
        
        if (prev == NULL || entry->epoch != epoch) {
                entry->epoch = epoch;        
                cur->_dep = NULL;
                if (cur->_type == SPLIT_WRITE) {
                        cur->_read_dep = NULL;
                        cur->_read_count = 0;
                }
                entry->key_struct = cur;
                assert(cur->_dep != action);        
        } else if (cur->_type == SPLIT_WRITE && prev->_type == SPLIT_READ) {
                cur->_dep = NULL;
                cur->_read_count = 0;
                entry->key_struct = cur;
                cur->_read_dep = prev;
                assert(cur->_dep != action);        
        } else if (cur->_type == SPLIT_READ && prev->_type == SPLIT_READ) {
                cur->_dep = prev->_dep;
                prev->_read_count += 1;
                cur->_read_dep = prev;
        } else if (cur->_type == SPLIT_READ && prev->_type == SPLIT_WRITE) {
                assert(cur->_type == SPLIT_READ);
                cur->_dep = prev->_action;
                cur->_read_count = 1;
                cur->_read_dep = cur;
                entry->key_struct = cur;
        } else if (cur->_type == SPLIT_WRITE && prev->_type == SPLIT_WRITE) {
                cur->_dep = prev->_action;
                cur->_read_count = 0;
                cur->_read_dep = NULL;
                entry->key_struct = cur;
                assert(cur->_dep != action);
        } else {
                assert(false);
        }
}

void split_executor::do_check()
{
        split_record *temp;
        split_dep *check;

        for (uint32_t i = 0; i < 1000; ++i) {
                temp = (split_record*)config.tables[0]->Get((uint64_t)i);
                assert(temp->key.key == (uint64_t)i);
                check = temp->key_struct;
                assert(check == NULL || check->_key.key == (uint64_t)i);
        }
}

void split_executor::schedule_single(split_action *action)
{
        uint32_t nreads, nwrites, i;

        assert(action->get_state() == split_action::UNPROCESSED);
        action->transition_scheduled();
        action->_dependencies = &dep_array[dep_index];
        action->_dep_index = 0;
        action->_outstanding_flag = false;

        nreads = action->_readset.size();
        for (i = 0; i < nreads; ++i) 
                schedule_operation(action->_readset[i], action, SPLIT_READ);
        
        nwrites = action->_writeset.size();
        for (i = 0; i < nwrites; ++i) 
                schedule_operation(action->_writeset[i], action, SPLIT_WRITE);
}

void split_executor::schedule_batch(split_action_batch batch)
{
        uint32_t i;
        //        uint64_t prev, cur;
        reset_dep();        
        for (i = 0; i < batch.num_actions; ++i) {
                //                prev = dep_index;
                schedule_single(batch.actions[i]);
                //                cur = dep_index;
                //                assert(cur - prev == batch.actions[i]->_writeset.size());
        }
        epoch += 1;
}

bool split_executor::check_ready(split_action *action)
{
        uint32_t num_deps, *dep_index, num_reads;
        //        bool ready;        
        split_action *dep;
        //        bool incr_outstanding;

        //        incr_outstanding = (action->_outstanding_flag == false);
        //        ready = true;
        dep_index = &action->_dep_index;
        num_deps = action->_readset.size() + action->_writeset.size();
        num_reads = action->_readset.size();

        while (*dep_index < num_reads) {
                dep = action->_dependencies[*dep_index]._dep;
                assert(dep != action);
                if (dep != NULL && 
                    dep->check_complete() == false) {
                        //                    process_action(dep) == false) {
//                         action->_outstanding_flag = true;
//                         if (incr_outstanding) 
//                                 num_outstanding += 1;
                        return false;
                }                
                *dep_index += 1;
        }

        while (*dep_index < num_deps) {
                assert(action->_dependencies[*dep_index]._type == SPLIT_WRITE);
                if (action->_dependencies[*dep_index]._read_dep != NULL) {
                        if (action->_dependencies[*dep_index]._read_dep->_read_count != 0) {
                                //        action->_outstanding_flag = true;
                                //if (incr_outstanding) 
                                //        num_outstanding += 1;
                                return false;
                        }
                } else {
                        dep = action->_dependencies[*dep_index]._dep;
                        if (dep != NULL && 
                            dep->check_complete() == false) {
                                //                            process_action(dep) == false) {
                                //action->_outstanding_flag = true;
                                //if (incr_outstanding)
                                //        num_outstanding += 1;
                                return false;
                        }                
                }
                *dep_index += 1;
        }

        //if (action->_outstanding_flag == true)
        //        num_outstanding -= 1;
        /*
        while (*dep_index != num_deps) {
                dep = action->_dependencies[*dep_index]._dep;
                assert(dep != action);
                if (dep != NULL && 
                    dep->get_state() != split_action::COMPLETE && 
                    process_action(dep) == false) {
                        ready = false;
                        break;
                }                
                *dep_index += 1;
        }
        */
        return true;
}

void split_executor::signal_reads(split_action *action)
{
        uint32_t i, nreads;
        split_dep *deps;

        nreads = action->_readset.size();
        deps = action->_dependencies;
        for (i = 0; i < nreads; ++i) {
                assert(deps[i]._type == SPLIT_READ);
                deps[i]._read_dep->_read_count -= 1;
        }
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
                                signal_reads(action);
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
        //        bool commit;

        if (check_ready(action)) {
                action->run();
                if (action->abortable() == true) 
                        action->transition_executed();
                        //sync_commit_rvp(action, commit);
                else 
                        action->transition_complete();
                schedule_downstream_pieces(action);
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
                
                //                if (num_outstanding > config.outstanding_threshold)
                //                        exec_pending();
                
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
