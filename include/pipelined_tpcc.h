#include <tpcc.h>
#include <pipelined_action.h>

namespace p_new_order {
        
        class warehouse_read : public txn {
        private:
                uint32_t 	_wh;
                float 		_wh_tax;
                
        public:
                warehouse_read(uint32_t wh);
                virtual bool Run();
                
                virtual uint32_t num_reads();
                virtual void get_reads(big_key *array);
                
                float get_tax();
        };

        class district_update : public txn {                
        private:
                uint32_t 	_wh;
                uint32_t 	_d;
                float 		_d_tax;
                uint32_t 	_next_o_id;

        public:
                district_update(uint32_t wh, uint32_t d);
                virtual bool Run();
                
                virtual uint32_t num_rmws();
                virtual void get_rmws(big_key *array);
                
                float get_tax();
                uint32_t get_order_id();
        };

        class customer_read : public txn {
        private:
                uint32_t 	_wh;
                uint32_t 	_d;
                uint32_t 	_c;
                float 		_c_discount;                
                
        public:
                customer_read(uint32_t wh, uint32_t d, uint32_t c);
                virtual bool Run();
                
                virtual uint32_t num_reads();
                virtual void get_reads(big_key *array);
                
                float get_discount();
        };

        class process_items : public txn {
        private:
                uint32_t 		_wh;
                uint32_t 		_d;
                uint32_t 		*_supplier_whs;
                uint32_t 		*_items;
                uint32_t	 	*_order_quantities;
                uint32_t 		_nitems;

                warehouse_read 		*_wh_txn;
                district_update 	*_d_txn;
                customer_read 		*_c_txn;

                void process_single(uint32_t n, uint32_t order_id, float w_tax, 
                                    float d_tax, 
                                    float c_disc);

        public:
                process_items(uint32_t wh, uint32_t d, uint32_t *supplier_whs, 
                              uint32_t *items, 
                              uint32_t *order_quantities,
                              uint32_t nitems,
                              warehouse_read *wh_txn,
                              district_update *d_txn,
                              customer_read *c_txn);
                virtual bool Run();
                
                virtual uint32_t num_rmws();
                virtual void get_rmws(big_key *array);                
        };
        
        class new_order_ins : public txn {
        private:
                uint32_t 		_wh;
                uint32_t 		_d;
                district_update 	*_dist_txn;                

        public:
                new_order_ins(uint32_t wh_id, uint32_t d_id, 
                              district_update *dist_txn);
                
                virtual bool Run();
        };

        class oorder_ins : public txn {
        private:
                uint32_t 		_wh;
                uint32_t 		_d;
                uint32_t 		_c;
                uint32_t 		_nitems;
                bool 			_all_local;
                
                district_update 	*_dist_txn;

        public:
                oorder_ins(uint32_t wh, uint32_t d, uint32_t c, uint32_t nitems,
                           bool all_local,
                           district_update *dist_txn);
                virtual bool Run();
        };
};

namespace p_payment {
        
        class warehouse_update : public txn {
        private:
                uint32_t 		_wh;
                float 			_h_amount;
                char 			*_wh_name;
                
        public:
                warehouse_update(uint32_t wh, float h_amount);
                virtual bool Run();

                virtual uint32_t num_rmws();
                virtual void get_rmws(big_key *array);
                
                char* get_wh_name();
        };
        
        class district_update : public txn {
        private:
                uint32_t 		_wh;
                uint32_t 		_d;
                float			_h_amount; 	
                char 			*_d_name;

        public:
                district_update(uint32_t wh, uint32_t d, float h_amount);
                virtual bool Run();

                virtual uint32_t num_rmws();
                virtual void get_rmws(big_key *array);
                
                char* get_d_name();
        };

        class customer_update : public txn {
        private:
                uint32_t 		_wh;
                uint32_t 		_d;
                uint32_t 		_c_wh;
                uint32_t 		_c_d;
                uint32_t 		_c;
                float 			_h_amount;

        public:
                customer_update(uint32_t wh, uint32_t d, uint32_t c_wh, 
                                uint32_t c_d, 
                                uint32_t c, 
                                float h_amount);
                virtual bool Run();
                
                virtual uint32_t num_rmws();
                virtual void get_rmws(big_key *array);
        };

        class history_ins : public txn {
        private:
                uint32_t 		_wh;
                uint32_t 		_d;
                
                uint32_t 		_c_wh;
                uint32_t 		_c_d;
                uint32_t 		_c;
                
                float 			_h_amount;
                uint32_t 		_time;
                
                warehouse_update 	*_wh_txn;
                district_update 	*_d_txn;
                
        public:
                history_ins(uint32_t wh, uint32_t d, uint32_t c_wh, uint32_t c_d,
                            uint32_t c_id, 
                            uint32_t time, 
                            float h_amount,
                            warehouse_update *wh_txn,
                            district_update *d_txn);
                virtual bool Run();
        };
};
