#include <split_action.h>
#include <util.h>
#include <cassert>

split_action_queue::split_action_queue()
{
        _sealed = false;
        _head = NULL;
        _tail = NULL;
}

void split_action_queue::reset()
{
        _sealed = false;
        _head = NULL;
        _tail = NULL;
}

void split_action_queue::seal()
{
        assert(_sealed == false);
        _sealed = true;
}

bool split_action_queue::is_empty()
{
        return (_head == NULL);
}

ready_queue::ready_queue() : split_action_queue()
{
}

void ready_queue::enqueue(split_action *action)
{
        assert((_head == NULL && _tail == NULL) || 
               (_head != NULL && _tail != NULL));
        assert(_sealed == false);
        
        action->transition_scheduled();
        action->ready_ptr = NULL;
        if (_head == NULL) 
                _head = action;
        else 
                _tail->ready_ptr = action;        
        _tail = action;
}

split_action* ready_queue::dequeue() 
{
        assert(_sealed == true);

        split_action *ret;
        ret = _head;
        
        if (_head != NULL)
                _head = _head->ready_ptr;
        return ret;
}

void ready_queue::merge_queues(ready_queue *merge_into,
                               ready_queue *merge_from)
{
        assert((merge_into->_head == NULL && merge_into->_tail == NULL) ||
               (merge_into->_head != NULL && merge_into->_tail != NULL));
        assert((merge_from->_head == NULL && merge_from->_tail == NULL) ||
               (merge_from->_head != NULL && merge_from->_tail != NULL));
        
        if (merge_from->is_empty() == true)
                return;

        if (merge_into->_head == NULL) {
                merge_into->_head = merge_from->_head;
                merge_into->_tail = merge_from->_tail;
        } else {
                assert(merge_into->_tail->ready_ptr == NULL);
                merge_into->_tail->ready_ptr = merge_from->_head;
                merge_into->_tail = merge_from->_tail;
        }
}

linked_queue::linked_queue() : split_action_queue()
{
}

void linked_queue::enqueue(split_action *action)
{
        assert((_head == NULL && _tail == NULL) || 
               (_head != NULL && _tail != NULL));
        assert(_sealed == false);
        
        action->link_ptr = NULL;
        if (_head == NULL) 
                _head = action;
        else 
                _tail->link_ptr = action;
        _tail = action;
}

split_action* linked_queue::dequeue()
{
        assert(_sealed == true);
        split_action *ret;
        
        ret = _head;
        if (_head != NULL) 
                _head = _head->link_ptr;
        return ret;
}

txn_graph* split_txn::convert_to_graph()
{
        throw unimplemented_exception(0);
}

split_action::split_action(txn *t, uint32_t partition_id, 
                           uint64_t dependency_flag, 
                           bool can_abort) : translator(t)
{
        this->t = t;
        this->partition_id = partition_id;
        this->dependency_flag = dependency_flag;
        
        this->num_pending_locks = 0;
        this->list_ptr = NULL;
        this->ready_ptr = NULL;
        this->link_ptr = NULL;
        this->done_locking = false;
        this->state = split_action::UNPROCESSED;
        this->can_abort = can_abort;
}

split_action::split_action_state split_action::get_state()
{
        return state;
}

bool split_action::ready()
{       
        bool has_deps;
        bool local_condition;
        
        local_condition = ((state == split_action::LOCKED) && 
                           (num_pending_locks == 0 || shortcut == true));
        if (local_condition == true) {
                barrier();
                has_deps = dependency_flag;
                barrier();
                return (has_deps == false);
        } else {
                return false;
        }
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
        assert(state == split_action::SCHEDULED);
        ret = t->Run();
        return ret;
}

/* XXX Incomplete */
void split_action::release_multi_partition()
{
}

bool split_action::abortable()
{
        return can_abort;
}

/* Returns true if action can commit */
bool split_action::can_commit()
{
        assert(can_abort == true);
        
        split_action_status status;
        status = (split_action_status)commit_rendezvous->status;        
        assert(status == ACTION_COMMITTED || status == ACTION_ABORTED);
        
        return (status == ACTION_COMMITTED);
}

/* Call this function _after_ logical locks have been acquired */
void split_action::transition_locked()
{
        /* Must transition from unprocessed */
        assert(state == split_action::UNPROCESSED);
        
        /* If abortable, cannot be shortcutted */
        assert(can_abort == false || shortcut == false);
        
        /* If not abortable, should be shortcutted or have some pending locks */
        //        assert(can_abort == true || shortcut == true || num_pending_locks > 0);
        state = split_action::LOCKED;
}

/* Call this function when the piece is ready to run */
void split_action::transition_scheduled()
{
        assert(state == split_action::LOCKED);
        
        /* Must be ready to execute */
        assert(ready() == true);
        state = split_action::SCHEDULED;
}

/* Call this function when an abortable piece has finished executing */
void split_action::transition_executed()
{
        assert(state == split_action::SCHEDULED);
        assert(can_abort == true);
        assert(shortcut == false);
        state = split_action::EXECUTED;        
}

/* 
 * Call this function when the piece has completed, and all state has been 
 * purged 
 */
void split_action::transition_complete()
{
        assert(can_abort == false || state == split_action::EXECUTED);
        assert(can_abort == true || state == split_action::SCHEDULED);
        state = split_action::COMPLETE;
}

commit_rvp* split_action::get_commit_rvp()
{
        return commit_rendezvous;
}

void split_action::set_commit_rvp(commit_rvp *rvp)
{
        commit_rendezvous = rvp;
}
