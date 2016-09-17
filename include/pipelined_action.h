#ifndef 	PIPELINED_ACTION_H_
#define 	PIPELINED_ACTION_H_

#include <locking_action.h>
#include <mcs_rw.h>
#include <pipelined_record.h>

#define NUM_TPCC_PIECES 	11
#define NUM_TPCC_TXNS		2

namespace pipelined {

class dep_node;

mcs_rw::mcs_rw_lock* get_lock(void* record);
locking_key** get_deplist(void *record);
void* get_record(void *record);

typedef enum {
        new_order = 0,
        payment = 1,
} txn_types;
 
typedef enum {
        new_order_warehouse = 0,
        new_order_district = 1,
        new_order_new_order = 2,
        new_order_customer = 3,
        new_order_stock = 4,
        new_order_oorder = 5,
        new_order_orderline = 6,
} new_order_pieces;

typedef enum {
        payment_warehouse = 0,
        payment_district = 1,
        payment_customer = 2,
        payment_history = 3,
} payment_pieces;

class action {
        
 private:
        //        char 			***_table;
        uint32_t 		_type;
        locking_action 		**_actions;
        uint32_t 		_num_actions;

 public:  
        action(uint32_t type, locking_action **lck_txns, uint32_t ntxns);
        uint32_t get_num_actions();
        uint32_t get_type();
        locking_action** get_actions();
};

};

#endif 		// PIPELINED_ACTION_H_

