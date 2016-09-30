#include <split_action.h>
#include <util.h>
#include <cassert>
#include <runnable.hh>

split_action::split_action(txn *t, uint32_t partition_id, 
                           uint64_t dependency_flag, 
                           bool can_abort, bool is_post) : translator(t)
{
        this->t = t;
        _partition_id = partition_id;
        _dependency_flag = dependency_flag;
        _state = (uint64_t)split_action::UNPROCESSED;
        _can_abort = can_abort;
        _is_post = is_post;
        _worker = NULL;
}

void split_action::set_worker(Runnable *worker)
{
        _worker = worker;
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
        _dep_rvp = rvp;
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

void* split_action::insert_ref(uint64_t key, uint32_t table_id)
{
        TableRecord *record;
        Table *tbl;

        record = _insert_mgr->get_table_record(table_id);
        record->key = key;
        record->next = NULL;
        tbl = _tables[table_id];
        tbl->Insert(key, record);
        return record->value;
}

void split_action::remove(__attribute__((unused)) uint64_t key, 
                          __attribute__((unused)) uint32_t table_id)
{
        assert(false);
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
                if (k->_key.key == key && k->_key.table_id == table_id && k->_value != NULL) {
                        return k->_value;
                }
        }
        
        assert(index == num_writes);
        return _tables[table_id]->Get(key);
}

/* XXX Incomplete */
void* split_action::read(uint64_t key, uint32_t table_id)
{
        uint32_t index, num_reads;
        split_dep *k;

        num_reads = _readset.size();
        for (index = 0; index < num_reads; ++index) {
                k = &_dependencies[index];
                if (k->_key.key == key && k->_key.table_id == table_id && k->_value != NULL) {
                        return k->_value;
                }
        }
        assert(index == num_reads);        
        return _tables[table_id]->Get(key);
}

uint64_t split_action::remote_deps()
{
        uint64_t cnt;
        if (_dependency_flag == 1) {
                assert(_dep_rvp != NULL);
                barrier();
                cnt = _dep_rvp->done;
                barrier();
                if (cnt == 1) {
                        _dependency_flag = 0;
                        return 0;
                } else {
                        return 1;
                }
        } else {
                return 0;
        }
}

int split_action::rand()
{
        /* XXX incomplete */
        assert(false);
        return 0;
}

uint64_t split_action::gen_guid()
{
        /* XXX incomplete */
        assert(_worker != NULL);
        _worker->gen_guid();
}

bool split_action::run()
{
        assert(_is_post == true || _state == split_action::SCHEDULED);

        bool ret;
        ret = t->Run();
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

bool split_action::check_complete()
{
        if (_can_abort && _state == split_action_state::EXECUTED) {
                if (_commit_rendezvous->status != ACTION_UNDECIDED) {
                        _state = split_action_state::COMPLETE;
                        return true;
                } else {
                        return false;
                }
        } else {
                return (_state == split_action_state::COMPLETE);
        }
}
