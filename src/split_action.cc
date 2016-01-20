#include <split_action.h>
#include <util.h>
#include <cassert>

txn_graph* split_txn::convert_to_graph()
{
        throw unimplemented_exception(0);
}

split_action::split_action(txn *t, uint32_t partition_id, 
                           uint64_t dependency_flag) : translator(t)
{
        this->t = t;
        this->partition_id = partition_id;
        this->dependency_flag = dependency_flag;
        
        this->num_pending_locks = 0;
        this->list_ptr = NULL;
        this->exec_list = NULL;        
        this->done_locking = false;
        this->state = split_action::UNPROCESSED;
}

split_action::split_action_state split_action::get_state()
{
        return state;
}

bool split_action::ready()
{       
        bool has_deps;
        if (done_locking == false)
                return false;

        barrier();
        has_deps = dependency_flag;
        barrier();
        
        return (has_deps == false) && (num_pending_locks == 0);
}

void split_action::set_lock_flag()
{
        assert(done_locking == false);
        done_locking = true;
}

void split_action::set_lock_list(void* list_ptr)
{
        this->list_ptr = list_ptr;
}

void* split_action::get_lock_list()
{
        return this->list_ptr;
}

void split_action::incr_pending_locks()
{
        this->num_pending_locks += 1;
}

void split_action::decr_pending_locks()
{
        this->num_pending_locks -= 1;
}

uint32_t split_action::get_partition_id()
{
        return this->partition_id;
}


/* RVP related functions */
void split_action::set_rvp(rendezvous_point *rvp)
{
        if (rvp == NULL) {
                this->rvp_sibling = NULL;
        } else {
                this->rvp_sibling = rvp->to_run;
                rvp->to_run = this;
        }
}

void split_action::clear_dependency_flag()
{
        assert(dependency_flag == true);
        barrier();
        dependency_flag = false;
        barrier();
}

void split_action::set_rvp_wakeups(rendezvous_point **rvps, uint32_t count)
{
        this->rvps = rvps;
        this->rvp_count = count;
}

split_action* split_action::get_rvp_sibling()
{
        return this->rvp_sibling;
}

uint32_t split_action::num_downstream_rvps()
{
        return this->rvp_count;
}

rendezvous_point** split_action::get_rvps()
{
        return this->rvps;
}

/* XXX Incomplete */
void* split_action::write_ref(__attribute__((unused)) uint64_t key, 
                              __attribute__((unused)) uint32_t table_id)
{
        return NULL;
}

/* XXX Incomplete */
void* split_action::read(__attribute__((unused)) uint64_t key, 
                         __attribute__((unused)) uint32_t table_id)
{
        return NULL;
}

/* XXX Incomplete */
int split_action::rand()
{
        return 0;
}

/* XXX Incomplete */
bool split_action::run()
{
        assert(state == split_action::UNPROCESSED);
        state = split_action::COMPLETE;
        return true;
}

/* XXX Incomplete */
void split_action::release_multi_partition()
{
}

