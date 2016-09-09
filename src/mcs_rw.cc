#include <mcs_rw.h>
#include <util.h>
#include <cassert>

void mcs_rw::acquire_writer(mcs_rw_lock *lock, mcs_rw_node *node)
{
        mcs_rw_node *pred;
        volatile uint64_t nreaders;

        node->_type = WRITER;
        node->_next = NULL;
        node->_state = create_state(0, NONE);
        
        barrier();
        
        pred = (mcs_rw_node*)xchgq((volatile uint64_t*)&lock->_tail, 
                                   (uint64_t)node);
        barrier();
        if (pred == NULL) {
                barrier();
                xchgq((volatile uint64_t*)&lock->_next_writer, 
                      (uint64_t) node);	/* next_writer */

                //                mfence();
                barrier();
                nreaders = lock->_nreaders;
                barrier();

                if (nreaders == 0 && 
                    (mcs_rw_node*)(xchgq((volatile uint64_t*)&lock->_next_writer,
                                         (uint64_t)NULL)) == node) {
                        set_unblocked(&node->_state);
                        return;
                } else {
                        goto spin_wait;
                }
        } else {
                node->_prev_type = pred->_type;
                barrier();
                set_type(&pred->_state, WRITER);
                xchgq((volatile uint64_t*)&pred->_next, (uint64_t)node);
        }
        
 spin_wait:
        barrier();
        spin_ready(node);
        barrier();
        assert(lock->_nreaders == 0);
}

void mcs_rw::acquire_reader(mcs_rw_lock *lock, mcs_rw_node *node)
{
        mcs_rw_node *prev, *next;
        node_state cmp_state, write_state;

        cmp_state = create_state(0, NONE);
        write_state = create_state(0, READER);
        
        node->_type = READER;
        node->_next = NULL;
        node->_state = create_state(0, NONE);

        barrier();
        
        prev = (mcs_rw_node*)xchgq((volatile uint64_t*)&lock->_tail, 
                                   (uint64_t)node);
        barrier();

        if (prev == NULL) {
                node->_prev_type = NONE;
                fetch_and_increment(&lock->_nreaders);
                set_unblocked(&node->_state);
        } else {
                node->_prev_type = prev->_type;
                barrier();
                if (prev->_type == WRITER ||
                    cmp_and_swap(&prev->_state, cmp_state, write_state)) {
                        xchgq((volatile uint64_t*)&prev->_next, (uint64_t)node);

                        barrier();
                        spin_ready(node);
                        barrier();

                } else {                        
                        fetch_and_increment(&lock->_nreaders);
                        barrier();
                        xchgq((volatile uint64_t*)&prev->_next, (uint64_t)node);
                        set_unblocked(&node->_state);
                }
        }
        
        if (get_type(node->_state) == READER) {
                barrier();
                next = spin_next(node);
                barrier();
                fetch_and_increment(&lock->_nreaders);
                set_unblocked(&next->_state);
        }
}

void mcs_rw::release_writer(mcs_rw_lock *lock, mcs_rw_node *node)
{
        mcs_rw_node *next;

        barrier();
        next = (mcs_rw_node*)node->_next;
        barrier();
        
        if (next != NULL || 
            cmp_and_swap((volatile uint64_t*)&lock->_tail, 
                         (uint64_t)node, 
                         (uint64_t)NULL) == false) {
                barrier();
                next = spin_next(node);
                barrier();
                if (get_type(next->_type) == READER) {
                        assert(lock->_nreaders == 0);
                        fetch_and_increment(&lock->_nreaders);
                }
                set_unblocked(&next->_state);
        }
}

void mcs_rw::release_reader(mcs_rw_lock *lock, mcs_rw_node *node)
{
        mcs_rw_node *next;
        
        barrier();
        next = (mcs_rw_node*)node->_next;
        barrier();

        /* node is the only queue element */
        if (next != NULL || 
            cmp_and_swap((volatile uint64_t*)&lock->_tail, 
                         (uint64_t)node, 
                         (uint64_t)NULL) == false) {
                barrier();
                next = spin_next(node);
                barrier();
                if (get_type(node->_state) == WRITER)
                        xchgq((volatile uint64_t*)&lock->_next_writer, 
                              (uint64_t)next);                
        }
        
        if (fetch_and_decrement(&lock->_nreaders) == 0) {
                next = (mcs_rw_node*)xchgq((volatile uint64_t*)&lock->_next_writer, 
                                           (uint64_t)NULL);
                if (next != NULL)
                        set_unblocked(&next->_state);
        }
}

mcs_rw::node_state mcs_rw::create_state(uint32_t blocked, mcs_type type)
{
        return ((uint64_t)blocked) << 32 | (type);
}

mcs_rw::mcs_type mcs_rw::get_type(node_state state)
{
        static uint64_t type_mask = 0x00000000FFFFFFFF;
        return (mcs_type)(state & type_mask);
}

void mcs_rw::set_type(volatile node_state *state, mcs_type type)
{
        static uint64_t type_mask = 0xFFFFFFFF00000000;
        node_state old_state, new_state;
        
        do {
                barrier();
                old_state = *state;
                barrier();
                
                new_state = (old_state & type_mask) | type;
        } while (cmp_and_swap((volatile uint64_t*)state, (uint64_t)old_state, 
                              (uint64_t)new_state) == false);
}

void mcs_rw::set_unblocked(volatile node_state *state)
{
        static uint64_t type_mask = 0x00000000FFFFFFFF;
        node_state old_state, new_state;
        
        do {
                barrier();
                old_state = *state;
                barrier();

                new_state = (old_state & type_mask) | (((uint64_t)1) << 32);
        } while (cmp_and_swap((volatile uint64_t*)state, 
                              (uint64_t)old_state, 
                              (uint64_t)new_state) == false);
}

mcs_rw::mcs_rw_node* mcs_rw::spin_next(mcs_rw_node *node)
{
        mcs_rw_node *next;
        while (true) {
                barrier();
                next = (mcs_rw_node*)node->_next;
                barrier();
                
                if (next != NULL)
                        return next;
        }
        return NULL;
}

void mcs_rw::spin_ready(mcs_rw_node *node)
{
        static uint64_t ready_mask = 0xFFFFFFFF00000000;
        node_state state;
        
        while (true) {
                barrier();
                state = node->_state;
                barrier();
                
                if ((state & ready_mask) != 0)
                        return;
        }
}
