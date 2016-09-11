#ifndef 	MCS_RW_H_
#define 	MCS_RW_H_

#include <stdint.h>
#include <machine.h>
#include <string>

namespace mcs_rw {

struct mcs_rw_node;

struct mcs_rw_lock {
        volatile uint64_t __attribute__((__packed__, __aligned__(CACHE_LINE))) 	_nreaders;
        volatile mcs_rw_node __attribute__((__packed__, __aligned__(CACHE_LINE)))  	*_next_writer;
        volatile mcs_rw_node __attribute__((__packed__, __aligned__(CACHE_LINE))) 	*_tail;
};

typedef enum {
        NONE = 0,
        READER = 1,
        WRITER = 2,
} mcs_type;

typedef uint64_t node_state;

struct mcs_rw_node {
        mcs_type 		_type;
        mcs_type 		_prev_type;
        volatile mcs_rw_node	*_next;
        volatile node_state 	 _state;        
};

/* Get a write lock */
void acquire_writer(mcs_rw_lock *lock, mcs_rw_node *node);

/* Release write lock */
void release_writer(mcs_rw_lock *lock, mcs_rw_node *node);

/* Get a read lock */
void acquire_reader(mcs_rw_lock *lock, mcs_rw_node *node);

/* Release read lock */
void release_reader(mcs_rw_lock *lock, mcs_rw_node *node);

/* Spin until lock is acquired */
void spin_ready(mcs_rw_node *node);

/* Spin for successor */
mcs_rw_node* spin_next(mcs_rw_node *node);

/* State manipulation functions */
node_state create_state(uint32_t blocked, mcs_type type);
mcs_type get_type(node_state state);
void set_type(volatile node_state *state, mcs_type type);
void set_unblocked(volatile node_state *state);
};

#endif 		// MCS_RW_H_
