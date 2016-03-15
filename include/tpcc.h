#ifndef 	TPCC_H_
#define 	TPCC_H_

#include <db.h>

#define 	NUM_STOCK_LEVEL_ORDERS 		20

class tpcc_key {
 private:
        static const uint32_t s_customer_shift =  	24;
        static const uint32_t s_district_shift = 	16;
        static const uint32_t s_new_order_shift =	24;
        static const uint32_t s_order_shift = 		24;
        static const uint32_t s_order_line_shift = 	56;
        static const uint32_t s_stock_shift = 		16;

        static const uint64_t s_customer_mask = 	0x000000FFFF000000;
        static const uint64_t s_district_mask = 	0x0000000000FF0000;
        static const uint64_t s_warehouse_mask =	0x000000000000FFFF;
        static const uint64_t s_stock_mask = 		0x0FFFFFFFFFFF0000;    
        static const uint64_t s_order_mask = 		0x00FFFFFFFF000000;

 public:
        static inline uint32_t get_stock_key(uint64_t composite_key) 
        {
            return (uint32_t)((composite_key & s_stock_mask) >> s_stock_shift);
        }

        static inline uint32_t get_customer_key(uint64_t composite_key) 
        {
            return (uint32_t)((composite_key & s_customer_mask) >> s_customer_shift);
        }

        static inline uint32_t get_warehouse_key(uint64_t composite_key) 
        {
            return (uint32_t)(composite_key & s_warehouse_mask);
        }

        static inline uint32_t get_district_key(uint64_t composite_key) 
        {
            return (uint32_t)((composite_key & s_district_mask) >> s_district_shift);
        }

        static inline uint32_t get_order_key(uint64_t composite_key) 
        {
            return (uint32_t)((composite_key & s_order_mask) >> s_order_shift);
        }

        // Expects: 	warehouse_id 	==> 	keys[0] 
        // 		district_id  	==> 	keys[1]
        //		customer_id  	==> 	keys[2]
        static inline uint64_t create_customer_key(uint32_t warehouse_id, 
                                                   uint32_t district_id, 
                                                   uint32_t customer_id)
        {
            return (
                    ((uint64_t)warehouse_id)				|
                    (((uint64_t)district_id) << s_district_shift)	|
                    (((uint64_t)customer_id) << s_customer_shift)		
                    );
        }

        // Expects: 	warehouse_id 	==> 	keys[0]
        //		district_id 	==> 	keys[1]
        static inline uint64_t
                create_district_key(uint32_t warehouse_id, 
                                    uint32_t district_id) 
        {
            return (
                    ((uint64_t)warehouse_id)				|
                    ((uint64_t)district_id << s_district_shift)
                    );
        }

        // Expects: 	warehouse_id 	==> 	keys[0]
        //		district_id 	==> 	keys[1]
        //		new_order_id	==> 	keys[2]
        static inline uint64_t create_new_order_key(uint32_t warehouse_id, 
                                                    uint32_t district_id, 
                                                    uint32_t new_order_id) 
        {
            return (
                    ((uint64_t)warehouse_id)   				|
                    ((uint64_t)district_id << s_district_shift)		|
                    ((uint64_t)new_order_id << s_new_order_shift)
                    );
        }	

        // Expects: 	warehouse_id 	==> 	keys[0]
        //		district_id 	==> 	keys[1]
        //		order_id	==> 	keys[2]
        static inline uint64_t create_order_key(uint32_t warehouse_id, 
                                                uint32_t district_id, 
                                                uint32_t order_id)
        {
            return (
                    ((uint64_t)warehouse_id)				|
                    (((uint64_t)district_id) << s_district_shift)	|
                    (((uint64_t)order_id) << s_order_shift)
                    );
        }

        static inline uint64_t create_order_line_key(uint32_t warehouse_id, uint32_t district_id
                                                     uint32_t *keys) 
        {            
            return (
                    ((uint64_t)keys[0]) 				| 
                    (((uint64_t)keys[1]) << s_district_shift) 		| 
                    (((uint64_t)keys[2]) << s_order_shift)		|
                    (((uint64_t)keys[3]) << s_order_line_shift)
                    );
        }

        static inline uint64_t create_stock_key(uint32_t *keys) 
        {
            return (
                    ((uint64_t)keys[0])				|
                    (((uint64_t)keys[1]) << s_stock_shift)		
                    );
        }
};

struct warehouse_record {
        uint32_t 	_w_id;
        float 		w_ytd;
        float 		w_tax;
        char 		w_name[11];
        char 		w_street_1[21];
        char 		w_street_2[21];
        char 		w_city[21];
        char 		w_state[4];
        char 		w_zip[10];
};

struct district_record {
        uint32_t	d_id;
        uint32_t	d_w_id;
        uint32_t	d_next_o_id;
        float 		d_ytd;
        float 		d_tax;
        char 		d_name[11];
        char 		d_street_1[21];
        char 		d_street_2[21];
        char 		d_city[21];
        char 		d_state[4];
        char 		d_zip[10];
};

struct customer_record {
        uint32_t	c_id;
        uint32_t	c_d_id;
        uint32_t	c_w_id;
        int 		c_payment_cnt;
        int 		c_delivery_cnt;
        char 		*c_since;
        float 		c_discount;
        float 		c_credit_lim;
        float 		c_balance;
        float 		c_ytd_payment;
        char 		c_credit[3];
        char 		c_last[16];
        char 		c_first[17];
        char 		c_street_1[21];
        char 		c_street_2[21];
        char 		c_city[21];
        char 		c_state[3];
        char 		c_zip[11];
        char 		c_phone[17];
        char 		c_middle[3];
        char 		c_data[501];
};

struct stock_record {
        uint32_t 		s_i_id; // PRIMARY KEY 2
        uint32_t 		s_w_id; // PRIMARY KEY 1
        uint32_t 		s_order_cnt;
        uint32_t 		s_remote_cnt;
        uint32_t 		s_quantity;
        float 			s_ytd;
        char 			s_data[51];
        char			s_dist_01[25];
        char 			s_dist_02[25];
        char			s_dist_03[25];
        char 			s_dist_04[25];    
        char 			s_dist_05[25];
        char 			s_dist_06[25];
        char 			s_dist_07[25];
        char 			s_dist_08[25];
        char  			s_dist_09[25];
        char 			s_dist_10[25];
};

struct item_record {
        uint32_t 		i_id; // PRIMARY KEY
        int 			i_im_id;
        float 			i_price;
        char 			i_name[25];
        char 			i_data[51];
};

struct history_record {
        uint32_t	h_c_id;
        uint32_t	h_c_d_id;
        uint32_t	h_c_w_id;
        uint32_t	h_d_id;
        uint32_t	h_w_id;
        uint32_t 	h_date;
        float 		h_amount;
        char		h_data[26];
};

struct oorder_record {
        uint32_t 	o_id;
        uint32_t 	o_w_id;
        uint32_t 	o_d_id;
        uint32_t 	o_c_id;
        uint32_t 	o_carrier_id;
        uint32_t 	o_ol_cnt;
        uint32_t 	o_all_local;
        long 		o_entry_d;
};

struct order_line_record { 
        uint32_t 	ol_w_id;
        uint32_t 	ol_d_id;
        uint32_t 	ol_o_id;
        uint32_t 	ol_number;
        uint32_t 	ol_i_id;
        uint32_t 	ol_supply_w_id;
        uint32_t 	ol_quantity;
        long 		ol_delivery_d;
        float 		ol_amount;
        char 		ol_dist_info[25];
};

struct new_order_record {
        uint32_t 	no_w_id;
        uint32_t 	no_d_id;
        uint32_t 	no_o_id;
};

class new_order : public txn {
        
        uint32_t 		_warehouse_id;
        uint32_t 		_district_id;
        uint32_t 		_customer_id;        
        vector<uint64_t> 	_items;
        vector<uint32_t> 	_order_quantities;
        vector<uint64_t> 	_supplier_warehouse_ids;        

 public:

        virtual bool Run();
        
};

class payment : public txn {

};

#endif 		// TPCC_H_
