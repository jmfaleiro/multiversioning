#include <split_action.h>
#include <util.h>
#include <cassert>

void split_txn::pre_execution()
{
}

void split_txn::post_execution()
{
}

split_action::split_action(txn *t, uint32_t partition_id, 
                           uint64_t dependency_flag, 
                           bool can_abort) : translator(t)
{
        this->t = t;
        _partition_id = partition_id;
        _dependency_flag = dependency_flag;
        _state = (uint64_t)split_action::UNPROCESSED;
        _can_abort = can_abort;
}

split_action::split_action_state split_action::get_state()
{
        return (split_action::split_action_state)_state;
}

uint32_t split_action::get_partition_id()
{
        return _partition_id;
}


/* RVP related functions */
void split_action::set_rvp(rendezvous_point *rvp)
{
        if (rvp == NULL) {
                _rvp_sibling = NULL;
        } else {
                _rvp_sibling = rvp->to_run;
                rvp->to_run = this;
        }
}

void split_action::clear_dependency_flag()
{
        uint64_t prev;

        prev = xchgq(&_dependency_flag, 0);
        assert(prev == 1);
}

void split_action::set_rvp_wakeups(rendezvous_point **rvps, uint32_t count)
{
        _rvps = rvps;
        _rvp_count = count;
}

split_action* split_action::get_rvp_sibling()
{
        return _rvp_sibling;
}

uint32_t split_action::num_downstream_rvps()
{
        return _rvp_count;
}

rendezvous_point** split_action::get_rvps()
{
        return _rvps;
}

void* split_action::write_ref(uint64_t key, uint32_t table_id)
{
        uint32_t index, num_writes, num_reads;
        split_dep *k;
        
        num_writes = _writeset.size();
        num_reads = _readset.size();
        num_writes += num_reads;
        for (index = num_reads; index < num_writes; ++index) {
                k = &_dependencies[index];
                if (k->_key.key == key && k->_key.table_id == table_id) {
                        return k->_value;
                }
        }
        assert(index < num_writes);
        return NULL;
}

/* XXX Incomplete */
void* split_action::read(uint64_t key, uint32_t table_id)
{
        uint32_t index, num_reads;
        split_dep *k;

        num_reads = _readset.size();
        for (index = 0; index < num_reads; ++index) {
                k = &_dependencies[index];
                if (k->_key.key == key && k->_key.table_id == table_id) {
                        return k->_value;
                }
        }
        assert(index < num_reads);
        return NULL;
}

uint64_t split_action::remote_deps()
{
        uint64_t dep_count;
        barrier();
        dep_count = _dependency_flag;
        barrier();
        return dep_count;
}

/* XXX Incomplete */
int split_action::rand()
{
        return 0;
}

/* XXX Incomplete */
bool split_action::run()
{
        assert(_state == split_action::SCHEDULED);

        bool ret;
        t->pre_execution();
        ret = t->Run();
        t->post_execution();
        return ret;
}

bool split_action::abortable()
{
        return _can_abort;
}

/* Call this function when the piece is scheduled */
void split_action::transition_scheduled()
{
        assert(_state == split_action::UNPROCESSED);
        _state = split_action::SCHEDULED;
}

/* Call this function when an abortable piece has finished executing */
void split_action::transition_executed()
{
        assert(_state == split_action::SCHEDULED);
        assert(_can_abort == true);
        xchgq(&_state, (uint64_t)split_action::EXECUTED);
}

/* 
 * Call this function when the piece has completed, and all state has been 
 * purged 
 */
void split_action::transition_complete()
{
        assert(_can_abort == false && _state == split_action::SCHEDULED);
        _state = split_action::COMPLETE;
}

void split_action::transition_complete_remote()
{
        assert(_can_abort == true);

        uint64_t val;
        val = (uint64_t)split_action::COMPLETE;
        val = xchgq(&_state, val);
        assert(val == (uint64_t)split_action::EXECUTED);
}

commit_rvp* split_action::get_commit_rvp()
{
        return _commit_rendezvous;
}

void split_action::set_commit_rvp(commit_rvp *rvp)
{
        _commit_rendezvous = rvp;
}

split_dep* split_action::get_dependencies()
{
        return _dependencies;
}

uint32_t* split_action::get_dep_index()
{
        return &_dep_index;
}
