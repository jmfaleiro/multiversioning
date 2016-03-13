#include <shared_split_executor.h>


bool shared_split_executor::check_ready(split_action *action)
{
        uint32_t *i, num_deps;
        split_dep *dependencies;

        i = action->get_dep_index();
        dependencies = action->get_dependencies();
        while (*i < num_deps) {
                if (!process_action(dependencies[*i]._action))
                        return false;
                *i += 1;
        }        
        return true;
}

bool shared_split_executor::process_action(split_action *action)
{
        assert(action != NULL);
        volatile uint64_t state;
        
        barrier();
        state = action->get_state();
        barrier();
        
        if (state != split_action::COMPLETE) {
                if (cmp_and_swap(&action->_state, split_action::UNPROCESSED, split_action::SCHEDULED) &&
                    state == split_action::SCHEDULED &&
                    action->remote_deps() == 0) {
                        if (try_exec(action) == true) {
                                return true;
                        } else {
                                xchgq(&action->_state, split_action::UNPROCESSED);
                                return false;
                        }
                } else {
                        return false;
                }
        } else { /* state == split_action::COMPLETE */
                return true;
        }
}

void shared_split_executor::sync_commit_rvp(split_action *action)
{
        commit_rvp *rvp;
        bool sched_rights;
        uint32_t i;
        split_action **to_notify;
        uint64_t prev_state;

        action->transition_executed();
        
        sched_rights = false;
        rvp = action->get_commit_rvp();
        to_notify = rvp->to_notify;
        if (fetch_and_increment(&rvp->num_committed) == rvp->num_actions) {
                sched_rights = true;
                prev_state = xchgq(&rvp->status, (uint64_t)ACTION_COMMITTED);
                assert(prev_state == ACTION_UNDECIDED);
        } 

        if (sched_rights == true) {
                schedule_downstream_pieces(action);
                
        }        
}

bool shared_split_executor::try_exec(split_action *action)
{
        assert(action->get_state() == split_action::SCHEDULED);
        if (check_ready(action) == true) {
                action->run();
                if (action->abortable() == true) 
                        sync_commit_rvp(action);
                else 
                        xchgq(&action->_state, split_action::COMPLETE);
                schedule_downstream_pieces(action);
                return true;
        } else {
                return false;
        }        
}

void shared_split_executor::exec_batch(split_action_batch batch)
{
        uint32_t i;
        
        for (i = _exec_id; i < batch.num_actions; i += num_execs) {
                if (!process_action(batch.actions[i])) 
                        add_pending(batch.actions[i]);
                
                while (_num_outstanding > config.max_outstanding)
                        exec_pending();
        }

        while(_num_outstanding > 0)
                exec_pending();
}
