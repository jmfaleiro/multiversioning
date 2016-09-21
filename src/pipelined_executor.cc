#include <pipelined_executor.h>
#include <locking_action.h>
#include <pipelined_action.h>
#include <lock_manager.h>
#include <record_buffer.h>
#include <mcs.h>
#include <insert_buf_mgr.h>
#include <tpcc.h>

using namespace pipelined;

extern size_t *tpcc_record_sizes;

locking_key* executor::get_marked_ref(locking_key *ref)
{
        uint64_t mask = 0x1;
        return (locking_key*)(((uint64_t)ref) | mask);
}

locking_key* executor::get_unmarked_ref(locking_key *ref)
{
        uint64_t mask = ~(0x1);
        return (locking_key*)(((uint64_t)ref) & mask);
}

bool executor::is_unmarked_ref(locking_key *key)
{
        uint64_t mask = 0x1;
        return ((((uint64_t)key) & mask) == 0);
}

void executor::mark_deleted(locking_key *key)
{
        volatile locking_key **prev;
        locking_key *prev_val, *new_val;
        
        assert(executor::is_unmarked_ref(key->prev) == true);
        prev = (volatile locking_key**)(&key->prev);
        
        while (true) {
                barrier();
                prev_val = (locking_key*)(*prev);
                barrier();
                
                new_val = get_marked_ref(prev_val);
                if (cmp_and_swap((volatile uint64_t*)prev, (uint64_t)prev_val, 
                                 (uint64_t)new_val))
                        return;
        }        
}

executor::executor(executor_config conf, RecordBuffersConfig rb_conf)
        : Runnable(conf._cpu)
{
        _conf = conf;
        _record_buffers = new(conf._cpu) RecordBuffers(rb_conf);
        _insert_buf_mgr = new(conf._cpu) insert_buf_mgr(conf._cpu, 11, 
                                                        tpcc_record_sizes);
        _dep_tbl = new dependency_table();
        init_depnodes();        
}

void* executor::operator new(std::size_t sz, int cpu)
{
        return alloc_mem(sz, cpu);
}

void executor::Init()
{
}

void executor::init_depnodes()
{
        uint32_t i;

        _depnode_list = (dep_node*)alloc_mem(10000*sizeof(dep_node), _conf._cpu);
        memset(_depnode_list, 0x0, sizeof(dep_node)*10000);
        for (i = 0; i < 10000; ++i) 
                _depnode_list[i]._next = &_depnode_list[i+1];
        _depnode_list[i-1]._next = NULL;
}

dep_node* executor::alloc_depnode()
{
        assert(_depnode_list != NULL);
        dep_node *ret;
        
        ret = _depnode_list;
        _depnode_list = _depnode_list->_next;
        ret->_next = NULL;
        return ret;
}

void executor::return_depnodes(dep_node *head, dep_node *tail)
{
        assert(head != NULL && tail != NULL);
        tail->_next = _depnode_list;
        _depnode_list = head;
}

void executor::StartWorking()
{
        action *txn;
        action_batch cur_batch;
        uint32_t i;
        bool loading;

        while (true) {
                cur_batch = _conf._input->DequeueBlocking();
                
                for (i = 0; i < cur_batch._batch_sz; ++i) 
                        exec_txn(cur_batch._txns[i]);

                _conf._output->EnqueueBlocking(cur_batch);
        }
}

/* 
 * Txn pieces are totally ordered. 
 * Foreach piece:
 * 
 * 1) Wait for the piece's dependencies to commit.
 * 2) Acquire the piece's locks.
 * 3) Execute the piece.
 * 4) Add the txn to records' reader/writer lists.
 * 5) Release the piece's locks.
 * 6) Find new transactions the current txn is dependent on
 */
void executor::exec_txn(action *txn)
{
        locking_action **sub_actions;
        uint32_t npieces;
        uint32_t i;
        
        prepare(txn);
        npieces = txn->get_num_actions();
        sub_actions = txn->get_actions();
        if (txn->get_type() == LOADER_TXN) {
                assert(npieces == 1);
                _conf._lck_mgr->Lock(sub_actions[0]);
                sub_actions[0]->Run();
                _conf._lck_mgr->Unlock(sub_actions[0]);
        } else {
                for (i = 0; i < npieces; ++i) {
                        wait_deps(txn, i);
                        _conf._lck_mgr->Lock(sub_actions[i]);
                        sub_actions[i]->Run();
                        add_dep(txn, i);
                        _conf._lck_mgr->Unlock(sub_actions[i]);
                        get_deps(txn, i);
                }
                finish_txn(txn);
        }
        clear_context();
}

void executor::prepare(action *txn)
{
        uint32_t npieces, nreads, nwrites, i, j;
        locking_action **sub_actions;

        npieces = txn->get_num_actions();
        sub_actions = txn->get_actions();
        set_context(txn);

        for (i = 0; i < npieces; ++i) {
                xchgq((volatile uint64_t*)&sub_actions[i]->status, UNEXECUTED);
                sub_actions[i]->tables = _conf._tbl_mgr;
                sub_actions[i]->insert_mgr = _insert_buf_mgr;
                sub_actions[i]->bufs = _record_buffers;
                sub_actions[i]->lck = &_mcs_lock_struct;
                sub_actions[i]->worker = this;

                nreads = sub_actions[i]->readset.size();
                for (j = 0; j < nreads; ++j) 
                        sub_actions[i]->readset[j].txn = txn;

                nwrites = sub_actions[i]->writeset.size();
                for (j = 0; j < nwrites; ++j) 
                        sub_actions[i]->writeset[j].txn = txn;
        }
}

void executor::finish_txn(action *txn)
{
        uint32_t npieces, nreads, nwrites, i, j;
        locking_action **sub_actions, *sub_txn;
        locking_key **deplist;

        npieces = txn->get_num_actions();
        sub_actions = txn->get_actions();
        
        for (i = 0; i < npieces; ++i) {
                sub_txn = sub_actions[i];

                nreads = sub_actions[i]->readset.size();
                for (j = 0; j < nreads; ++j) {
                        deplist = get_deplist(sub_txn->readset[j].value);
                        remove_single(&sub_txn->readset[j], deplist);
                }
                
                nwrites = sub_actions[i]->writeset.size();
                for (j = 0; j < nwrites; ++j) {
                        deplist = get_deplist(sub_txn->writeset[j].value);
                        remove_single(&sub_txn->writeset[j], deplist);
                }
        }
}

/* Get new transactions "txn" is dependent on due to the operation in "start". */
void executor::get_operation_deps(action *txn, locking_key *start)
{
        if (start->is_write == true) {
                if (start->prev != NULL) {
                        if (start->prev->is_write == true) 
                                add_prev_write(txn, start);
                        else 
                                add_prev_reads(txn, start);
                }
        } else {
                add_prev_write(txn, start);
        }
}

/* Add preceding writer to txn's dependency list */
void executor::add_prev_write(action *txn, locking_key *start)
{
        locking_key *pred;
        dep_node *cur_dep;
        locking_action_status st;

        assert(start->dep_status == (uint64_t)INSERT_COMPLETE);
        assert(executor::is_unmarked_ref(start->prev) == true);
        
        for (pred = start->prev; pred != NULL && pred->is_write == false; 
             pred = executor::get_unmarked_ref(pred->prev)) {
                wait_inserted(pred);
        }
        
        if (pred != NULL) {// && executor::is_unmarked_ref(pred->prev) == true) {
                assert(pred->is_write == true && pred->txn != NULL);
                add_dep_context((pipelined::action*)pred->txn);
        }
}

/* Add preceding readers to txn's dependency list */
void executor::add_prev_reads(action *txn, locking_key *start)
{
        locking_key *pred;
        dep_node *cur_dep;

        assert(start->dep_status == (uint64_t)INSERT_COMPLETE);
        assert(executor::is_unmarked_ref(start->prev) == true);

        for (pred = start->prev; pred != NULL && pred->is_write == false; 
             pred = executor::get_unmarked_ref(pred->prev)) {
                assert(pred->is_write == false && pred->txn != NULL);
                //                if (executor::is_unmarked_ref(pred->prev))
                add_dep_context((pipelined::action*)pred->txn);
                wait_inserted(pred);
        }
}

/* 
 * Find newly created dependencies due to piece's execution. 
 * 
 * Locking keys should have already been added at this point.
 */
void executor::get_deps(action *txn, uint32_t piece) 
{
        locking_action *sub_txn;
        uint32_t nreads, nwrites, nactions, i;
        
        sub_txn = (txn->get_actions())[piece];

        nreads = sub_txn->readset.size();
        for (i = 0; i < nreads; ++i) 
                get_operation_deps(txn, &sub_txn->readset[i]);

        nwrites = sub_txn->writeset.size();
        for (i = 0; i < nwrites; ++i) 
                get_operation_deps(txn, &sub_txn->writeset[i]);
}

/*
 * Wait for pieces of transactions "txn" is dependent on to finish executing.
 */
void executor::wait_deps(action *txn, uint32_t piece)
{
        dep_node *nodes;
        action *dep_txn;
        locking_action *to_wait;
        locking_action_status st;

        assert(txn == _context._txn);
        nodes = _context._head;
        while (nodes != NULL) {
                dep_txn = nodes->_txn;
                assert(dep_txn != NULL);
                to_wait = get_dependent_piece(txn, dep_txn, piece);
                assert(to_wait != NULL);
                do {
                        barrier();
                        st = to_wait->status;
                        barrier();
                } while (st != COMPLETE);
                
                nodes = nodes->_next;
        }
}

locking_action* executor::get_dependent_piece(action *txn, action *dependent_piece,
                                              uint32_t piece)
{
        uint32_t pc_index;
        locking_action **sub_txns;

        pc_index = _dep_tbl->get_dependent_piece(txn->get_type(), 
                                                 dependent_piece->get_type(),
                                                 piece);
        sub_txns = dependent_piece->get_actions();
        return sub_txns[pc_index];
}

void executor::add_single_dep(locking_key *to_add, locking_key **head)
{
        assert(((uint64_t)to_add) % 2 == 0);
        
        locking_key *pred;
        
        to_add->dep_status = (uint64_t)PRE_INSERT;
        to_add->prev = NULL;
        barrier();
        pred = (locking_key*)xchgq((volatile uint64_t*)head, (uint64_t)to_add);
        to_add->prev = pred;
        assert(((uint64_t)pred) % 2 == 0);
        barrier();
        to_add->dep_status = (uint64_t)INSERT_COMPLETE;
        barrier();
}

void executor::wait_inserted(locking_key *key)
{
        uint64_t state;

        while (true) {
                barrier();
                state = key->dep_status;
                barrier();
                
                if (state == INSERT_COMPLETE)
                        return;
        }
}

void executor::remove_single(locking_key *to_remove, locking_key **head)
{
        volatile locking_key **cmp_ptr;
        locking_key **iter;
        locking_key *temp, *cmp_value;        
        
        executor::mark_deleted(to_remove);
        
 restart_remove:

        cmp_ptr = (volatile locking_key**)head;
        iter = head;
        
        while (true) {
                barrier();
                temp = *iter;
                barrier();

                if (is_unmarked_ref(temp)) {
                        cmp_ptr = (volatile locking_key**)iter;
                        cmp_value = temp;
                }
                
                temp = get_unmarked_ref(temp);
                if (temp == to_remove || temp == NULL) 
                        break;
                
                wait_inserted(temp);
                iter = &(temp->prev);
        }

        if (temp == to_remove) {
                if (cmp_and_swap((volatile uint64_t*)cmp_ptr, (uint64_t)cmp_value,
                                 (uint64_t)get_unmarked_ref(to_remove->prev)))
                        return;
                else
                        goto restart_remove;
        }
}

/*
 * Add this txn to the list of record readers and writers.
 * Later conflicting txns must take dependencies on this txn.
 */
void executor::add_dep(action *txn, uint32_t piece)
{
        locking_action *sub_txn;
        locking_key **deplist, *cur, *prev;
        uint32_t nreads, nwrites, i;

        sub_txn = (txn->get_actions())[piece];
        nreads = sub_txn->readset.size();
        for (i = 0; i < nreads; ++i) {
                deplist = get_deplist(sub_txn->readset[i].value);
                cur = &sub_txn->readset[i];
                add_single_dep(cur, deplist);
        }

        nwrites = sub_txn->writeset.size();
        for (i = 0; i < nwrites; ++i) {
                deplist = get_deplist(sub_txn->writeset[i].value);
                cur = &sub_txn->writeset[i];
                add_single_dep(cur, deplist);
        }
}

void executor::add_dep_context(action *txn)
{
        assert(txn != _context._txn);
        assert(txn != NULL);
        assert((_context._head == NULL && _context._tail == NULL) ||
               (_context._head != NULL && _context._tail != NULL));

        dep_node *node;

        for (node = _context._head; node != NULL && node->_txn != txn; node = node->_next)
                ;

        if (node == NULL) {
                node = alloc_depnode();
                assert(node != NULL);

                node->_next = NULL;
                node->_txn = txn;
                if (_context._head == NULL) 
                        _context._head = node;
                else 
                        _context._tail->_next = node;
                _context._tail = node;
        }
}

void executor::set_context(action *txn)
{
        assert(_context._txn == NULL && _context._head == NULL && 
               _context._tail == NULL);
        _context._txn = txn;
}

void executor::clear_context()
{
        assert(_context._txn != NULL);
        assert((_context._head == NULL && _context._tail == NULL) ||
               (_context._head != NULL && _context._tail != NULL));
        
        _context._txn = NULL;
        if (_context._head != NULL)
                return_depnodes(_context._head, _context._tail);
        _context._head = NULL;
        _context._tail = NULL;
}

dependency_table::dependency_table()
{
        uint32_t new_order_size, payment_size, i, j;

        new_order_size = tpcc_config::txn_sizes[NEW_ORDER_TXN];
        payment_size = tpcc_config::txn_sizes[PAYMENT_TXN];
        
        _tbl = (uint32_t***)zmalloc(sizeof(uint32_t**)*2);

        _tbl[NEW_ORDER_TXN] = (uint32_t**)zmalloc(sizeof(uint32_t*)*new_order_size);
        for (i = 0; i < new_order_size; ++i) {
                _tbl[NEW_ORDER_TXN][i] = (uint32_t*)zmalloc(sizeof(uint32_t)*2);
                _tbl[NEW_ORDER_TXN][i][NEW_ORDER_TXN] = i;
                if (i < 3)
                        _tbl[NEW_ORDER_TXN][i][PAYMENT_TXN] = i;
                else
                        _tbl[NEW_ORDER_TXN][i][PAYMENT_TXN] = 2;
        }

        _tbl[PAYMENT_TXN] = (uint32_t**)zmalloc(sizeof(uint32_t*)*payment_size);
        for (i = 0; i < payment_size; ++i) {
                _tbl[PAYMENT_TXN][i] = (uint32_t*)zmalloc(sizeof(uint32_t)*2);
                _tbl[PAYMENT_TXN][i][PAYMENT_TXN] = i;
                if (i < 3)
                        _tbl[PAYMENT_TXN][i][NEW_ORDER_TXN] = i;
                else 
                        _tbl[PAYMENT_TXN][i][NEW_ORDER_TXN] = 2;
        }
}

uint32_t dependency_table::get_dependent_piece(uint32_t dependent_type, 
                                               uint32_t dependency_type, 
                                               uint32_t piece_num)
{
        return _tbl[dependent_type][piece_num][dependency_type];
}
