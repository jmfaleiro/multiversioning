#ifndef 	SPLIT_TPCC_H_
#define 	SPLIT_TPCC_H_

#include <db.h>
#include <tpcc.h>
#include <vector>

namespace split_new_order {

        class update_district;
        class read_customer_last_name;
        class read_customer;
        class insert_new_order;
        class insert_oorder;
        class insert_order_lines;
        class update_stocks;

        struct stock_update_data {
                uint32_t 	_item_id;
                uint32_t 	_quantity;
                uint32_t 	_supplier_wh;
                char 		*_district_info;
        };

        class read_warehouse : public txn {
        private:
                friend class read_customer_last_name;
                friend class read_customer;
                friend class insert_new_order;
                friend class insert_oorder;
                friend class insert_order_lines;
                friend class update_stocks;

                uint32_t 		_warehouse_id;
                float 			_w_tax;

        public:
                read_warehouse(uint32_t warehouse_id);
                bool Run();
                
                virtual uint32_t num_reads();
                virtual void get_reads(big_key *array);
        };
        
        /* Root piece */
        class update_district : public txn {
        private:
                friend class read_customer_last_name;
                friend class read_customer;
                friend class insert_new_order;
                friend class insert_oorder;
                friend class insert_order_lines;
                friend class update_stocks;
                friend class read_warehouse;
                
                uint32_t 		_warehouse_id;
                uint32_t 		_district_id;
                uint32_t 		_order_id;
                
        public:
                update_district(uint32_t warehouse_id, uint32_t district_id);
                bool Run();
                
                virtual uint32_t num_rmws();
                virtual void get_rmws(big_key *array);
        };
        
        /* Root piece */
        class read_customer : public txn {
        private:
                friend class update_district;
                friend class read_customer_last_name;
                friend class insert_new_order;
                friend class insert_oorder;
                friend class insert_order_lines;
                friend class update_stocks;
                friend class read_warehouse;

                uint32_t 		_wh_id;
                uint32_t 		_d_id;
                bool 			_use_name;
                uint64_t 		_c_nm_idx;
                uint32_t 		_c_id;
                float 			_dscnt;		/* Need to write */

        public: 
                read_customer(uint32_t wh_id, uint32_t d_id, bool use_name, 
                              uint64_t name_index, 
                              uint32_t id);
                bool Run();

                virtual uint32_t num_reads();
                virtual void get_reads(big_key *array);
        };

        /* Must wait on update_district piece */
        class insert_new_order : public txn {
        private:
                friend class update_district;
                friend class read_customer_last_name;
                friend class read_customer;
                friend class insert_oorder;
                friend class insert_order_lines;
                friend class update_stocks;
                friend class read_warehouse;

                update_district 		*_dstrct_pc;	/* Read order id */
                read_customer			*_cust_pc;
                std::vector<update_stocks*>	_stock_pieces;
                uint32_t 		_wh_id;
                uint32_t 		_dstrct_id;
                bool 			_all_local;
                uint32_t 		_num_items;

                void do_new_order();
                void do_oorder();
                void do_order_lines();

        public:
                insert_new_order(update_district *dstrct_pc, 
                                 read_customer *customer_pc,
                                 uint32_t wh_id, 
                                 uint32_t dstrct_id, 
                                 bool all_local, 
                                 uint32_t num_items,
                                 std::vector<update_stocks*> *stock_pcs);

                virtual bool Run();

                virtual uint32_t num_rmws();
                virtual void get_rmws(big_key *array);
        };

        /* Must wait on update_district piece */
        class insert_oorder : public txn {
        private:
                friend class update_district;
                friend class read_customer_last_name;
                friend class read_customer;
                friend class insert_new_order;
                friend class insert_order_lines;
                friend class update_stocks;
                friend class read_warehouse;
                
                update_district 	*_dstrct_pc;	/* Read order id */
                read_customer 		*_cust_pc;
                uint32_t 		_wh_id;
                uint32_t 		_dstrct_id;
                bool 			_all_local;
                uint32_t 		_num_items;

        public:
                insert_oorder(update_district *dstrct_pc, 
                              read_customer *cust_pc,
                              uint32_t wh_id, 
                              uint32_t dstrct_id, 
                              bool all_local, 
                              uint32_t num_items);
                virtual bool Run();

                virtual uint32_t num_rmws();
                virtual void get_rmws(big_key *array);
        };
        
        
        class insert_order_lines : public txn {
        private:
                friend class update_district;
                friend class read_customer_last_name;
                friend class read_customer;
                friend class insert_new_order;
                friend class insert_oorder;
                friend class update_stocks;
                friend class ready_warehouse;

                uint32_t 			_wh_id;
                uint32_t 			_dstrct_id;	
                update_district 		*_dstrct_pc;	/* order id */
                std::vector<update_stocks*> 	_stock_pieces;  /* stock data */
                read_warehouse 			*_wh_pc;

        public:
                insert_order_lines(uint32_t wh_id, 
                                   uint32_t dstrct_id, 
                                   update_district *dstrct_pc,
                                   std::vector<update_stocks*> *stock_pieces);
                virtual bool Run();

                virtual uint32_t num_rmws();
                virtual void get_rmws(big_key *array);
        };

        /* Root piece. */
        class update_stocks : public txn {
        private:
                friend class update_district;
                friend class read_customer_last_name;
                friend class read_customer;
                friend class insert_new_order;
                friend class insert_oorder;
                friend class insert_order_lines;
                friend class read_warehouse;
                
                uint32_t 				_wh_id;
                uint32_t 				_dstrct_id;
                std::vector<stock_update_data> 		_info;

        public:
                uint32_t 				_partition;

                update_stocks(uint32_t wh_id, uint32_t dstrct_id, 
                              uint32_t partition,
                              stock_update_data *info, 
                              uint32_t num_stocks);

                virtual bool Run();

                virtual uint32_t num_rmws();
                virtual void get_rmws(big_key *array);
        };
};

namespace split_payment {
        class update_warehouse;
        class update_district;
        class update_customer;
        class insert_history;
        
        class update_warehouse : public txn {
        private:
                friend class update_district;
                friend class update_customer;
                friend class insert_history;
                
                uint32_t 	_wh_id;
                char 		*_warehouse_name;
                float 		_h_amount;

        public:
                update_warehouse(uint32_t wh_id, float h_amount);
                bool Run();
        };

        class update_district : public txn {
        private:
                friend class update_warehouse;
                friend class update_customer;
                friend class insert_history;
                
                uint32_t 	_warehouse_id;
                uint32_t 	_district_id;
                char		*_district_name;
                float 		_h_amount;
                
        public: 
                update_district(uint32_t warehouse_id, uint32_t district_id, 
                                float h_amount);
                bool Run();
        };

        class update_customer : public txn {
        private:
                friend class update_warehouse;
                friend class update_district;
                friend class insert_history;
                
                uint32_t 	_warehouse_id;
                uint32_t 	_district_id;
                uint32_t 	_customer_id;
                uint64_t 	_customer_name_ptr;                
                uint32_t 	_customer_district_id;
                uint32_t 	_customer_warehouse_id;
                bool 		_use_name;
                float 		_h_amount;

        public:
                update_customer(uint32_t warehouse_id, uint32_t district_id, 
                                uint32_t customer_id,
                                uint64_t customer_name,
                                bool use_name, 
                                float h_amount);
                bool Run();
        };

        class insert_history : public txn {
        private:
                friend class update_warehouse;
                friend class update_district;
                friend class update_customer;
                
                update_warehouse	*_warehouse_piece;
                update_district 	*_district_piece;
                uint32_t 		_wh_id;
                uint32_t 		_d_id;
                uint32_t 		_customer_id;
                uint32_t 		_customer_warehouse_id;
                uint32_t 		_customer_district_id;
                uint64_t 		_customer_name_ptr;
                bool 			_use_name;
                float 			_amount;
                uint32_t 		_time;

        public:
                insert_history(update_warehouse *warehouse_piece, 
                               update_district *district_piece, 
                               uint32_t warehouse_id,
                               uint32_t district_id,
                               uint32_t customer_id,
                               uint32_t customer_warehouse_id,
                               uint32_t customer_district_id,
                               uint64_t customer_name_ptr,
                               bool use_name, 
                               float amount,
                               uint32_t time);                               
                bool Run();
        };
};

// namespace split_stock_level {
//         class read_district;
//         class read_oorder;
//         class read_order_lines;
//         class read_stocks;
// 
//         struct order_info {
//                 uint32_t 	_order_id;
//                 uint32_t 	_item_count;
//         };
// 
//         class read_district : public txn {
//         private:
//                 friend class read_oorder;
//                 friend class read_order_lines;
//                 friend class read_stocks;
//                 
//                 uint32_t 	_warehouse_id;
//                 uint32_t 	_district_id;
//                 uint32_t 	_order_id;	/* for reads */
// 
//         public:
//                 read_district(uint32_t warehouse_id, uint32_t district_id);
//                 bool Run();
//         };
//         
//         class read_oorder : public txn {
//                 friend class read_district;
//                 friend class read_order_lines;
//                 friend class read_stocks;
// 
//         private:
//                 read_district 		*_district_piece;
//                 uint32_t 		_warehouse_id;
//                 uint32_t		_district_id;                
//                 uint32_t 		_order_id;		/* to set */
//                 order_info 		*_orders;		/* to set */
//                 uint32_t 		_num_orders;
// 
//         public:
//                 read_oorder(read_district *district_piece, 
//                             uint32_t warehouse_id, 
//                             uint32_t district_id);
//                             
//                 bool Run();
//         };
//         
//         class read_order_lines : public txn {
//                 friend class read_district;
//                 friend class read_oorder;
//                 friend class read_stocks;
//                 
//         private:
//                 read_oorder 		*_oorder_piece;	/* for order id */
//                 uint32_t 		_warehouse_id;
//                 uint32_t 		_district_id;
//                 uint32_t	  	*_item_ids;
//                 uint32_t 		_num_items;
//         public:
//                 read_order_lines(read_oorder *oorder_piece, 
//                                  uint32_t warehouse_id, 
//                                  uint32_t district_id);
//                 bool Run();
//         };
// 
//         class read_stocks : public txn {
//                 friend class read_district;
//                 friend class read_oorder;
//                 friend class read_order_lines;
//                 
//         private:
//                 read_order_lines	*_order_lines_piece;	/* for items */
//                 uint32_t 		_warehouse_id;
//                 uint32_t 		_district_id;
//                 uint32_t 		_threshold;
//                 volatile uint32_t 	_under_threshold_count;
//                 
//         public:
//                 read_stocks(read_order_lines *order_lines, 
//                             uint32_t warehouse_id, 
//                             uint32_t district_id);
//                 bool Run();
//         };
// };
// 
// namespace split_order_status {
//         class read_oorder_index;
//         class read_oorder;
//         class read_order_lines;
//         
//         class read_oorder_index : public txn {
//                 friend class read_oorder;
//                 friend class read_order_lines;
//                 
//         private:               
//                 uint32_t 	_wh_id;
//                 uint32_t 	_dstrct_id;
//                 uint32_t 	_cust_id;
//                 uint64_t 	_cust_nm_idx;
//                 bool 		_use_name;
//                 uint32_t 	_order_id;	/* Needs to be set */
// 
//         public:
//                 read_oorder_index(uint32_t warehouse_id, uint32_t district_id, 
//                                   uint32_t cust_id, 
//                                   uint64_t cust_name_idx, 
//                                   bool use_name);
//                 bool Run();
//         };
//         
//         class read_oorder : public txn {
//                 friend class read_oorder_index;
//                 friend class read_order_lines;
//                 
//         private:                
//                 read_oorder_index 	*_oorder_idx_pc;/* order id */
//                 uint32_t 		_wh_id;
//                 uint32_t 		_dstrct_id;
//                 uint32_t 		_order_id;	/* needs to be set */
//                 uint32_t 		_num_items;	/* needs to be set */
//                 
//         public:
//                 read_oorder(read_oorder_index *oorder_idx_pc, uint32_t wh_id, 
//                             uint32_t dstrct_id);
//                 bool Run();                
//         };
// 
//         class read_order_lines : public txn {
//                 friend class read_oorder_index;
//                 friend class read_oorder;
//                 
//         private:
//                 read_oorder 		*_oorder_piece;	/* needed for items */
//                 uint32_t 		_wh_id;
//                 uint32_t 		_dstrct_id;
//                 volatile uint32_t 	_ol_quantity;
// 
//         public:                
//                 read_order_lines(read_oorder *oorder_piece, uint32_t wh_id, 
//                                  uint32_t dstrct_id);
//                 bool Run();
//         };
// };
// 
// namespace split_delivery {
//         class update_last_delivery;
//         class read_oorder;
//         class read_order_line;
//         class update_customer;
//         class remove_new_order;
//         
//         class update_last_delivery : public txn {
//                 friend class read_oorder;
//                 friend class read_order_line;
//                 friend class update_customer;
//                 friend class remove_new_order;
//                 
//         private:
//                 uint32_t 		_wh_id;
//                 std::vector<uint32_t> 	_d_ids;
//                 std::vector<uint32_t> 	_to_deliver;
// 
//         public:
//                 update_last_delivery(uint32_t warehouse_id);
//                 bool Run();
//                 
//         };
// 
//         /* One of these txns runs per-district */
//         class read_oorder : public txn {
//                 friend class update_last_delivery;
//                 friend class read_order_line;
//                 friend class update_customer;
//                 friend class remove_new_order;
//                 
//         private:
//                 update_last_delivery	*_delivery_index_piece;
//                 uint32_t 		_w_id;
//                 uint32_t 		_d_id;
//                 uint32_t		_num_items;	/* Needs to be set */
//                 uint32_t 		_customer_id; 	/* Needs to be set */
//                 uint32_t 		_order_id;	/* Needs to be set */
// 
//         public:
//                 read_oorder(update_last_delivery *delivery_piece, 
//                             uint32_t warehouse_id, 
//                             uint32_t district_id);
// 
//                 bool Run();
//         };
//         
//         /* One of these per-district */
//         class read_order_line : public txn {
//                 friend class update_last_delivery;
//                 friend class read_oorder;
//                 friend class update_customer;
//                 friend class remove_new_order;
//                 
//         private:
//                 read_oorder 		*_oorder_piece;
//                 uint32_t 		_w_id;
//                 uint32_t 		_d_id;
//                 uint32_t 		_customer_id;
//                 float 			_amount;                
// 
//         public:
//                 read_order_line(read_oorder *oorder_piece, 
//                                 uint32_t w_id, 
//                                 uint32_t d_id);
// 
//                 bool Run();
//         };
// 
//         class update_customer : public txn {
// 
//                 friend class update_last_delivery;
//                 friend class read_oorder;
//                 friend class read_order_line;
//                 friend class remove_new_order;
// 
//         private:                
//                 read_order_line		*_ordr_ln_pc;	/* Get amount due */
//                 uint32_t 		_w_id;
//                 uint32_t 		_d_id;
// 
//         public:
//                 update_customer(read_order_line *order_line_piece, 
//                                 uint32_t w_id, 
//                                 uint32_t d_id);
//                 bool Run();
//         };
// 
//         class remove_new_order : public txn {
//                 friend class update_last_delivery;
//                 friend class read_oorder;
//                 friend class read_order_line;
//                 friend class update_customer;
// 
//         private:
//                 update_last_delivery	*_dlvry_pc;	/* Get order id */
//                 uint32_t 		_w_id;
//                 uint32_t 		_d_id;
// 
//         public:
//                 remove_new_order(update_last_delivery *dlvry_pc, 
//                                  uint32_t w_id, 
//                                  uint32_t d_id);
//                 bool Run();
//         };
// };
// 

#endif 		// SPLIT_TPCC_H_
