#include <tpcc.h>
#include <split_tpcc.h>

/* update_district is a "root" piece. */
bool split_new_order::update_district::Run()
{
        uint64_t district_key;
        district_record *district;

        district_key = tpcc_util::create_district_key(_warehouse_id, 
                                                      _district_id);
        district = get_write_ref(district_key, DISTRICT_TABLE);
        _order_id = district->d_next_o_id;
        district->d_next_o_id += 1;
}

/* read_customer is a "root" piece */
bool split_new_order::read_customer::Run()
{
        uint64_t customer_key;
        
        customer_key = tpcc_util::create_customer_key(_warehouse_id, 
                                                      _district_id, 
                                                      _customer_id);
}

/* Depends on the update_stocks and update_district pieces */
bool split_new_order::insert_order_lines::Run()
{
        order_line_record order_line;
        uint32_t i, order_id, num_stock_pieces, count;
        update_stocks *stock_piece;
        stock_update_data *stock_data;
        item_record *item;

        order_line.ol_o_id = order_id;
        order_line.ol_d_id = _district_id;
        order_line.ol_w_id = _warehouse_id;
        
        
        order_id = district_piece->_order_id;
        num_stock_pieces = stock_pieces.size();
        count = 0;
        for (i = 0; i < num_stock_pieces; ++j) {
                stock_piece = stock_pieces[i];
                num_stocks = stock_piece->_info.size();
                for (j = 0; j < num_stocks; ++j) {
                        order_line.ol_number = count;
                        count += 1;
                        stock_data = &stock_piece->_info[j];
                        order_line.ol_supply_w_id = stock_data->_supplier_wh;
                        order_line.ol_quantity = stock_data->_quantity;
                        order_line.ol_i_id = stock_data->_item_id;
                        item = (item_record*)get_read_ref((uint64_t)stock_data->_item_id, ITEM_TABLE);
                        order_line.ol_amount = item->i_price*stock_data->_quantity;
                        strcpy(order_line.ol_dist_info, stock_data->_district_info);
                }
        }
}

/* update_stocks is a "root" piece. */
bool split_new_order::update_stocks::Run()
{
        uint32_t i;
        uint64_t stock_key;
        stock_record *stock;
        
        for (i = 0; i < _num_stocks; ++i) {
                stock_key = tpcc_util::create_stock_key(_supplier_whs[i],
                                                        _stocks[i]);
                stock = get_write_ref(stock_key, STOCK_TABLE);
                if (stock->s_order_cnt - _order_quantities[i] >= 10)
                        stock->s_quantity -= _order_quantities[i];
                else 
                        stock->s_quantity += 91 - _order_quantities[i];
                
                if (_warehouse_id != _supplier_whs[i]) 
                        stock->s_remote_cnt += 1;
                
                stock->s_ytd += order_quantity;
                switch (_district_id) {
                case 1:
                        _dist_infos[i] = stock->s_dist_01;
                        break;
                case 2:
                        _dist_infos[i] = stock->s_dist_02;
                        break;
                case 3:
                        _dist_infos[i] = stock->s_dist_03;
                        break;
                case 4:
                        _dist_infos[i] = stock->s_dist_04;
                        break;
                case 5:
                        _dist_infos[i] = stock->s_dist_05;
                        break;
                case 6:
                        _dist_infos[i] = stock->s_dist_06;
                        break;
                case 7:
                        _dist_infos[i] = stock->s_dist_07;
                        break;
                case 8:
                        _dist_infos[i] = stock->s_dist_08;
                        break;
                case 9:
                        _dist_infos[i] = stock->s_dist_09;
                        break;
                case 10:
                        _dist_infos[i] = stock->s_dist_10;
                        break;
                default:	/* Shouldn't get here */
                        assert(false);
                }                
        }
        return true;
}

/* Depends on the execution of the update_district piece */
bool split_new_order::insert_oorder::Run()
{
        oorder_record oorder;
        uint64_t oorder_key;
        
        oorder_key = tpcc_util::create_new_order_key(_warehouse_id, 
                                                     _district_id,
                                                     district_piece->_order_id);
        oorder.o_id = district_piece->_order_id;
        oorder.o_w_id = _warehouse_id;
        oorder.o_d_id = _district_id;
        oorder.o_c_id = _customer_id;
        oorder.o_carrier_id = 0;
        oorder.o_ol_cnt = _items.size();
        oorder.o_all_local = _all_local;        
        insert_record(oorder_key, OORDER_TABLE, &oorder);
}

/* Depends on the execution of the update_district piece. */
bool split_new_order::insert_new_order::Run()
{
        uint32_t order_id;
        uint64_t new_order_key;
        new_order_record new_order;

        order_id = district_piece->_order_id;
        new_order.no_w_id = _warehouse_id;
        new_order.no_d_id = _district_id;
        new_order.no_o_id = order_id;

        new_order_key = tpcc_util::create_new_order_key(_warehouse_id, 
                                                        _district_id, 
                                                        order_id);
        insert_record(new_order_key, NEW_ORDER_TABLE, &new_order);
        return true;
}

/* This is a root piece */
bool split_payment::update_warehouse::Run()
{
        assert(_warehouse_id < tpcc_config::num_warehouses);
        warehouse_record *warehouse;
        
        warehouse = (warehouse_record*)get_write_ref((uint64_t)_warehouse_id, 
                                                     WAREHOUSE_TABLE);
        warehouse->w_ytd += _h_amount;
        _warehouse_name = warehouse->w_name;
        return true;
}

/* This is a root piece */
bool split_payment::update_district::Run()
{
        assert(_district_id >= 1 && _district_id <= 10);
        district_record *district;
        uint64_t district_key;
        
        district_key = create_district_key(_warehouse_id, _district_id);
        district = (district_record*)get_write_ref(district_key, 
                                                   DISTRICT_TABLE);
        district->d_ytd += _h_amount;
        _district_name = district->d_name;
        return true;
}

/* This is a root piece */
bool split_payment::update_customer::Run()
{
        uint64_t customer_key;
        customer_record *customer;        
        static const char *credit = "BC";
        static const char *space = " ";
        char c_id_str[17], c_d_id_str[17], c_w_id_str[17], d_id_str[17] \
                w_id_str[17], h_amount_str[17];
        
        /* Read the customer record */
        customer_key = create_customer_key(_warehouse_id, _district_id, 
                                           _customer_id);
        cust = (customer_record*)get_write_ref(customer_key, 
                                                   CUSTOMER_TABLE);

        /* Check credit */        
        if (strcmp(credit, cust->c_credit) == 0) {	
                
                /* Bad credit */
                sprintf(c_id_str, "%x", _customer_id);
                sprintf(c_d_id_str, "%x", _customer_district_id);
                sprintf(c_w_id_str, "%x", _customer_warehouse_id);
                sprintf(d_id_str, "%x", _warehouse_id);
                sprintf(w_id_str, "%x", _district_id);
                sprintf(h_amount_str, "%lx", (uint64_t)m_h_amount);
                
                static const char *holder[11] = {c_id_str, space, c_d_id_str, 
                                                 space, c_w_id_str, space, 
                                                 d_id_str, space, w_id_str, 
                                                 space, h_amount_str};
                tpcc_util::append_strings(cust->c_data, holder, 501, 11);
        } else {
                
                /* Good credit */
                cust->c_balance -= _h_amount;
                cust->c_ytd_payment += _h_amount;
                cust->c_payment_cnt += 1;
        }
        return true;
}

/* Depends on the update_warehouse and update_district pieces */
bool split_payment::insert_history::Run()
{
        history_record hist;
        static const char *empty = "    ";
        char *holder[3] = {_warehouse_piece->_warehouse_name, 
                           empty, 
                           _district_piece->_district_name};
        
        hist.h_c_id = _customer_id;
        hist.h_c_d_id = _customer_district_id;
        hist.h_c_w_id = _customer_warehouse_id;
        hist.h_d_id = _district_id;
        hist.h_w_id = _warehouse_id;
        hist.h_date = _time;
        hist.h_amount = _amount;
        tpcc_util::append_strings(hist.h_data, holder, 26, 3);
        insert_record(history_key, HISTORY_TABLE, &hist);
        return true;
}

split_stock_level::read_district::read_district(uint32_t warehouse_id, 
                                                uint32_t district_id)
{
        _warehouse_id = warehouse_id;
        _district_id = district_id;
}

/* Read the appropriate district record's next order id field. */
bool split_stock_level::read_district::Run()
{
        uint64_t district_key;
        district_record *district;

        district_key = tpcc_util::create_district_key(_warehouse_id, 
                                                      _district_id);
        district = get_read_ref(district_key, DISTRICT_TABLE);
        _order_id = district->d_next_o_id - 1;
        return true;
}

split_stock_level::read_oorder::read_oorder(read_district *district_piece, 
                                            uint32_t warehouse_id, 
                                            uint32_t district_id)
{
        assert(warehouse_id < tpcc_config::num_warehouses);
        assert(district_id >= 1 && district_id <= 10);
        assert(district_piece != NULL);
        assert(district_piece->_warehouse_id == warehouse_id);
        assert(district_piece->_district_id == district_id);

        _district_piece = district_piece;
        _warehouse_id = warehouse_id;
        _district_id = district_id;        
}

bool split_stock_level::read_oorder::Run()
{
        uint32_t max_order;
        uint64_t oorder_key;
        oorder_record *oorder;

        max_order = _district_piece->_order_id;
        assert(max_order > NUM_STOCK_LEVEL_ORDERS);
        _order_id = max_order;
        for (uint32_t i = 0; i < NUM_STOCK_LEVEL_ORDERS; ++i) {
                oorder_key = tpcc_util::create_new_order_key(_warehouse_id, 
                                                             _district_id, 
                                                             _order_id - i);
                oorder = (oorder_record*)get_read_ref(oorder_key, OORDER_TABLE);
                assert(oorder != NULL);
                _orders[i]._order_id = _order_id - i;
                _orders[i]._item_count = oorder->o_ol_cnt;
        }
        return true;
}

split_stock_level::read_order_lines::read_order_lines(read_oorder *oorder_piece,
                                                      uint32_t warehouse_id, 
                                                      uint32_t district_id)
{
        assert(warehouse_id < tpcc_config::num_warehouses);
        assert(district_id >= 1 && district_id <= 10);
        assert(oorder_piece->_warehouse_id == warehouse_id);
        assert(oorder_piece->_district_id == district_id);

        _warehouse_id = warehouse_id;
        _district_id = district_id;
}

/* Read every order line record */
bool split_stock_level::read_order_lines::Run()
{
        uint64_t ol_key;
        uint32_t i, j, count;
        order_info *orders, cur_order;
        order_line_record *ol;
        
        count = 0;
        orders = oorder_piece->orders;
        for (i = 0; i < NUM_STOCK_LEVEL_ORDERS; ++i) {
                cur_order = orders[i];
                for (j = 0; j < cur_order.item_count; ++j) {
                        ol_key = tpcc_util::create_order_line_key(_warehouse_id,
                                                                  _district_id, 
                                                                  orders[i]._order_id,
                                                                  j);
                        ol = (order_line_record*)get_read_ref(ol_key, 
                                                              ORDER_LINE_TABLE);
                        _item_ids[count] = ol->ol_i_id;
                        count += 1;
                }
        }
        _num_items = count;
        return true;
}

split_stock_level::read_stocks::read_stocks(read_order_lines *order_lines,
                                            uint32_t warehouse_id, 
                                            uint32_t district_id)
{
        _order_lines_piece = order_lines;
        _warehouse_id = warehouse_id;
        _district_id = district_id;
}

/* Check the levels of each stock we're interested in */
bool split_stock_level::read_stocks::Run()
{
        uint32_t total, last_proc, num_items, i, *item_ids, item;
        uint64_t stock_key;
        stock_record *stock;

        item_ids = _order_lines_piece->_item_ids;
        num_items = _order_lines_piece->_num_items;
        
        /* Sort stocks to avoid duplicates */
        std::sort(item_ids, &item_ids[num_items]);
        
        total = 0;
        for (i = 0; i < num_items; ++i) {
                item = item_ids[i];
                if (last_proc == item) 
                        continue;
                stock_key = tpcc_util::create_stock_key(_warehouse_id, 
                                                        _district_id, 
                                                        item_ids[i]);
                stock = (stock_record*)get_read_ref(stock_key, STOCK_TABLE);
                if (stock->s_quantity < _threshold)
                        total += 1;
                last_proc = item_ids[i];
        }
        _under_threshold_count = total;
        return true;
}

split_order_status::read_oorder_index::read_oorder_index(uint32_t warehouse_id, 
                                                         uint32_t district_id, 
                                                         uint32_t customer_id,
                                                         uint64_t customer_name_index,
                                                         bool use_name)
{
        _wh_id = warehouse_id;
        _dstrct_id = district_id;
        _cust_id = customer_id;
        _cust_nm_idx = customer_name_index;
        _use_name = use_name;
}

bool split_order_status::read_oorder_index::Run()
{
        uint64_t customer_key;

        customer_key = tpcc_util::create_customer_key(_wh_id, _dstrct_id, 
                                                      _cust_id);
        _last_order = *(uint64_t*)get_read_ref(customer_key, CUSTOMER_ORDER_INDEX);
        return true;
}

split_order_status::read_oorder::read_oorder()
{
        
}

bool split_order_status::read_oorder::Run()
{
        uint64_t order_key;
        oorder_record *oorder;
        
        order_key = _oorder_idx_pc->_last_order;
        oorder = (oorder_record*)get_read_ref(order_key, OORDER_TABLE);
        _num_items = oorder->o_ol_cnt;
        return true;
}

split_order_status::read_order_lines::read_order_lines(read_oorder *oorder_piece,
                                                       uint32_t wh_id, 
                                                       uint32_t d_id)
{
        _wh_id = wh_id;
        _d_id = d_id;
        _oorder_piece = oorder_piece;
}

bool split_order_status::read_order_lines::Run()
{
        uint32_t num_items, i, quantity;
        uint64_t ol_key;
        order_line_record *ol;

        num_items = _oorder_piece->_num_items;
        quantity = 0;
        for (i = 0; i < num_items; ++i) {
                ol_key = tpcc_util::create_order_line_key(_warehouse_id, 
                                                          _district_id, 
                                                          order_id, 
                                                          i);
                ol = (order_line_record*)get_read_ref(ol_key, ORDER_LINE_TABLE);
                quantity += ol->ol_quantity;
        }
        _order_line_quantity = quantity;
        return true;
}

split_delivery::update_last_delivery::update_last_delivery(uint32_t wh_id)
{
        assert(wh_id < tpcc_config::num_warehouses);
        uint32_t i;
        
        _wh_id = wh_id;
        for (i = 0; i < NUM_DISTRICTS; ++i) 
                _delivery_order_ids.push_back(0);
}

bool split_delivery::update_last_delivery::Run()
{
        uint32_t i, num_districts, d_id;
        uint64_t *delivery_order, d_key;
        district_reocrd *district;

        num_districts = _district_ids.size();
        for (i = 0; i < num_districts; ++i) {
                d_id = _district_ids[i];
                d_key = tpcc_util::create_district_key(_wh_id, _d_id);
                delivery_order = (uint64_t*)get_write_ref(d_key, DELIVERY_TABLE);
                district = (district_record*)get_read_ref(d_key, DISTRICT_TABLE);
                if (district->d_next_o_id - *deliver_order > 1) {
                        *delivery_order += 1;
                        *_to_deliver[d_id] = *deliver_order;
                }
        }
        return true;
}

split_delivery::read_oorder::read_oorder(update_last_delivery *delivery_index_piece, 
                                         uint32_t w_id,
                                         uint32_t d_id)
{
        assert(w_id < tpcc_config::num_warehouses);
        assert(id < 10);
        
        _delivery_index_piece = delivery_index_piece;
        _w_id = w_id;
        _d_id = d_id;
}

bool split_delivery::read_oorder::Run()
{
        uint64_t order_key;
        oorder_record *oorder;

        _order_id = _delivery_index_piece->_to_deliver[_d_id];
        order_key = tpcc_util::create_new_order_key(_w_id, _d_id, _order_id);
        oorder = (oorder_record*)get_read_ref(order_key, OORDER_TABLE);
        _num_items = oorder->o_ol_cnt;
        _customer_id = oorder->o_c_id;
        return true;
}

split_delivery::delete_new_order::delete_new_order(update_last_delivery *delivery_index_piece,
                                                   uint32_t w_id, 
                                                   uint32_t d_id)
{
        _delivery_index_piece = delivery_index_piece;
        _w_id = w_id;
        _d_id = d_id;
}

bool split_delivery::delete_new_order::Run()
{
        uint64_t order_key;
        uint32_t order_id;
        
        order_id = _delivery_index_piece->_to_deliver[_d_id];
        tpcc_util::create_new_order_key(_w_id, _d_id, order_id);
        delete_record(order_key, NEW_ORDER_TABLE);
        return true;
}

split_delivery::read_order_line::read_order_line(read_oorder *oorder_piece, 
                                                 uint32_t w_id,
                                                 uint32_t d_id)
{
        _oorder_piece = oorder_piece;
        _w_id = w_id;
        _d_id = d_id;
}

bool split_delivery::read_order_line::Run()
{       
        uint32_t i, n_items, order_id;
        uint64_t ol_key;
        order_line_record *ol;

        /* Reads from remote piece */
        _customer_id = _oorder_piece->_customer_id;
        n_items = _oorder_piece->_num_items;
        order_id = _oorder_piece->_order_id;
        
        /* Sum order line amounts */
        _amount = 0;
        for (i = 0; i < n_items; ++i) {
                ol_key = tpcc_util::create_order_line_key(_w_id, _d_id, order_id);
                ol = (order_line_record*)get_read_ref(ol_key, ORDER_LINE_TABLE);
                _amount += ol->ol_amount;
        }        
        return true;
}

split_delivery::update_customer::update_customer(read_order_line *order_line_piece,
                                                 uint32_t w_id,
                                                 uint32_t d_id)
{
        _ordr_ln_pc = order_line_piece;
        _w_id = w_id;
        _d_id = d_id;
}

bool split_delivery::update_customer::Run()
{
        uint64_t cust_key;
        uint32_t cust_id;
        float amount;
        customer_record *cust;

        cust_id = _ordr_ln_pc->_customer_id;
        amount = _ordr_ln_pc->_amount;
        cust_key = tpcc_util::create_customer_key(_w_id, _d_id, cust_id);
        cust = (customer_record*)get_read_ref(cust_key, CUSTOMER_TABLE);
        cust->c_delivery_cnt += 1;
        cust->c_balance += amount;        
        return true;
}
