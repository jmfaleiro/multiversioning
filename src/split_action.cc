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

void* split_action::write_ref(uint64_t key, uint32_t table_id)
{
        uint32_t index, num_writes;
        split_key *k;
        
        num_writes = writeset.size();
        for (index = 0; index < num_writes; ++index) {
                k = &writeset[index];
                if (k->_record.key == key && k->_record.table_id == table_id) 
                        break;
        }
        assert(index != num_writes);
        
        if (k->_value == NULL) 
                k->_value = tables[k->_record.table_id]->Get(k->_record.key);
        assert(k->_value != NULL);
        return k->_value;
}

/* XXX Incomplete */
void* split_action::read(uint64_t key, uint32_t table_id)
{
        uint32_t index, num_reads;
        split_key *k;

        num_reads = readset.size();
        for (index = 0; index < num_reads; ++index) {
                k = &readset[index];
                if (k->_record.key == key && k->_record.table_id == table_id) 
                        break;
        }
        assert(index != num_reads);
        
        if (k->_value == NULL) 
                k->_value = tables[k->_record.table_id]->Get(k->_record.key);
        assert(k->_value != NULL);
        return k->_value;
}

bool split_action::shortcut_flag()
{
        return shortcut;
}

void split_action::set_shortcut_flag()
{
        shortcut = true;
}

void split_action::reset_shortcut_flag()
{
        shortcut = false;
}

bool split_action::remote_deps()
{
        return dependency_flag;
}

/* XXX Incomplete */
int split_action::rand()
{
        return 0;
}

/* XXX Incomplete */
bool split_action::run()
{
        bool ret;
        assert(state == split_action::UNPROCESSED);
        ret = t->Run();
        state = split_action::COMPLETE;
        return ret;
}

/* XXX Incomplete */
void split_action::release_multi_partition()
{
}

