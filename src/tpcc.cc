#include <tpcc.h>
#include <cpuinfo.h>
#include <algorithm>

setup_tpcc* setup_tpcc::gen_wh_txn(uint32_t low, uint32_t high)
{
        setup_tpcc *ret;
        
        ret = new setup_tpcc();
        ret->_typ = setup_tpcc::WAREHOUSE;
        ret->_low = low;
        ret->_high = high;
        
        return ret;
}

setup_tpcc* setup_tpcc::gen_d_txn(uint32_t wh, uint32_t low, uint32_t high)
{
        setup_tpcc *ret;
        
        ret = new setup_tpcc();
        ret->_typ = setup_tpcc::DISTRICT;
        ret->_wh = wh;
        ret->_low = low;
        ret->_high = high;

        return ret;
}

setup_tpcc* setup_tpcc::gen_c_txn(uint32_t wh, uint32_t dstrct, 
                                  uint32_t low, 
                                  uint32_t high)
{
        setup_tpcc *ret;
        
        ret = new setup_tpcc();
        ret->_typ = setup_tpcc::CUSTOMER;
        ret->_wh = wh;
        ret->_dstrct = dstrct;
        ret->_low = low;
        ret->_high = high;

        return ret;
}

setup_tpcc* setup_tpcc::gen_i_txn(uint32_t low, uint32_t high)
{
        setup_tpcc *ret;
        
        ret = new setup_tpcc();
        ret->_typ = setup_tpcc::ITEM;
        ret->_low = low;
        ret->_high = high;
        return ret;
}

setup_tpcc* setup_tpcc::gen_s_txn(uint32_t wh, uint32_t low, uint32_t high)
{
        setup_tpcc *ret;
        
        ret = new setup_tpcc();
        ret->_typ = setup_tpcc::STOCK;
        ret->_wh = wh;
        ret->_low = low;
        ret->_high = high;
        return ret;
}

int setup_tpcc::gen_rand_range(int min, int max)
{
        int range;
        
        range = max - min + 1;
        return min + (txn_rand() % range);
}

void setup_tpcc::gen_rand_string(int min, int max, char *buf)
{
        int ch_first, ch_last, length, i;
        
        ch_first = 'a';
        ch_last = 'z';
        length = gen_rand_range(min, max);
        for (i = 0; i < length; ++i) 
                buf[i] = (char)gen_rand_range(ch_first, ch_last);
        buf[length] = '\0';
}

bool setup_tpcc::gen_warehouse(uint32_t wh_id)
{
        warehouse_record wh;
        char zip[] = "123456789", *record;

        wh.w_id = wh_id;
        wh.w_ytd = 3000;
        wh.w_tax = (txn_rand() % 2001) / 1000.0;
                
        gen_rand_string(6, 10, wh.w_name);
        gen_rand_string(10, 20, wh.w_street_1);
        gen_rand_string(10, 20, wh.w_street_2);
        gen_rand_string(10, 20, wh.w_city);
        gen_rand_string(3, 3, wh.w_state);
        strcpy(wh.w_zip, zip);
                
        if (get_write_ref((uint64_t)wh_id, WAREHOUSE_TABLE, (void**)&record) == false)
                return false;
        memcpy(record, &wh, sizeof(warehouse_record));
        return true;
}

bool setup_tpcc::gen_district(uint32_t wh_id, uint32_t d_id)
{
        uint64_t key;
        district_record d;
        char contiguous_zip[] = "123456789", *record;        
        
        d.d_id = d_id;
        d.d_w_id = wh_id;
        d.d_ytd = 3000;
        d.d_tax = (txn_rand() % 2001) / 1000.0;
        d.d_next_o_id = 3000;

        gen_rand_string(6, 10, d.d_name);
        gen_rand_string(10, 20, d.d_street_1);
        gen_rand_string(10, 20, d.d_street_2);
        gen_rand_string(10, 20, d.d_city);
        gen_rand_string(3, 3, d.d_state);

        strcpy(d.d_zip, contiguous_zip);
        key = tpcc_util::create_district_key(wh_id, d_id);
        if (get_write_ref(key, DISTRICT_TABLE, (void**)&record) == false)
                return false;
        memcpy(record, &d, sizeof(district_record));
        return true;
}

bool setup_tpcc::gen_item(uint32_t item_id)
{
        item_record itm;
        int rand_pct, len, original_start;
        char *record;

        itm.i_id = item_id;
        gen_rand_string(14, 24, itm.i_name);
        itm.i_price = (100 + (rand() % 9900)) / 100.0;
        rand_pct = gen_rand_range(0, 99);
        len = gen_rand_range(26, 50);

        gen_rand_string(len, len, itm.i_data);
        if (rand_pct <= 10) {

                // 10% of the time i_data has "ORIGINAL" crammed somewhere in the
                // middle. 
                original_start = gen_rand_range(2, len-8);
                itm.i_data[original_start] = 'O';
                itm.i_data[original_start+1] = 'R';
                itm.i_data[original_start+2] = 'I';
                itm.i_data[original_start+3] = 'G';
                itm.i_data[original_start+4] = 'I';
                itm.i_data[original_start+5] = 'N';
                itm.i_data[original_start+6] = 'A';
                itm.i_data[original_start+7] = 'L';
        }

        itm.i_im_id = 1 + (rand() % 10000);
        if (get_write_ref((uint64_t)item_id, ITEM_TABLE, (void**)&record) == false)
                return false;
        memcpy(record, &itm, sizeof(item_record));
        return true;
}

bool setup_tpcc::gen_stock(uint32_t wh_id, uint32_t item_id)
{
        stock_record stck;
        int rand_pct, len, start_original;
        uint64_t key;
        char *record;

        stck.s_i_id = item_id;
        stck.s_w_id = wh_id;
        stck.s_quantity = 10 + txn_rand() % 90;
        stck.s_ytd = 0;
        stck.s_order_cnt = 0;
        stck.s_remote_cnt = 0;

        /* s_data */
        rand_pct = gen_rand_range(1, 100);
        len = gen_rand_range(26, 50);

        gen_rand_string(len, len, stck.s_data);
        if (rand_pct <= 10) {

                // 10% of the time, i_data has the string "ORIGINAL" crammed 
                // somewhere in the middle.
                start_original = gen_rand_range(2, len-8);
                stck.s_data[start_original] = 'O';
                stck.s_data[start_original+1] = 'R';
                stck.s_data[start_original+2] = 'I';
                stck.s_data[start_original+3] = 'G';
                stck.s_data[start_original+4] = 'I';
                stck.s_data[start_original+5] = 'N';
                stck.s_data[start_original+6] = 'A';
                stck.s_data[start_original+7] = 'L';            
        }

        gen_rand_string(24, 24, stck.s_dist_01);
        gen_rand_string(24, 24, stck.s_dist_02);
        gen_rand_string(24, 24, stck.s_dist_03);
        gen_rand_string(24, 24, stck.s_dist_04);
        gen_rand_string(24, 24, stck.s_dist_05);
        gen_rand_string(24, 24, stck.s_dist_06);
        gen_rand_string(24, 24, stck.s_dist_07);
        gen_rand_string(24, 24, stck.s_dist_08);
        gen_rand_string(24, 24, stck.s_dist_09);
        gen_rand_string(24, 24, stck.s_dist_10);

        key = tpcc_util::create_stock_key(wh_id, item_id);
        if (get_write_ref(key, STOCK_TABLE, (void**)&record) == false)
                return false;
        memcpy(record, &stck, sizeof(stock_record));
        return true;
}

bool setup_tpcc::gen_customer(uint32_t wh_id, uint32_t d_id, uint32_t c_id)
{
        uint64_t key;
        customer_record c;
        uint32_t i;
        char *record;
        
        c.c_id = c_id;
        c.c_d_id = d_id;
        c.c_w_id = wh_id;
        c.c_discount = (rand() % 5001) / 10000.0;        

        if (txn_rand() % 101 <= 10) {		// 10% Bad Credit
                c.c_credit[0] = 'B';
                c.c_credit[1] = 'C';
                c.c_credit[2] = '\0';
        }
        else {		// 90% Good Credit
                c.c_credit[0] = 'G';
                c.c_credit[1] = 'C';
                c.c_credit[2] = '\0';
        }                
        gen_rand_string(8, 16, c.c_first);
        
        /* XXX NEED THIS TO LOOKUP CUSTOMERS BY LAST NAME */
        //        random.gen_last_name_load(c.c_last);
        //        s_last_name_index->Put(customer.c_last, &customer);

        c.c_credit_lim = 50000;
        c.c_balance = -10;
        c.c_ytd_payment = 10;
        c.c_payment_cnt = 1;
        c.c_delivery_cnt = 0;        

        gen_rand_string(10, 20, c.c_street_1);
        gen_rand_string(10, 20, c.c_street_2);
        gen_rand_string(10, 20, c.c_city);
        gen_rand_string(3, 3, c.c_state);
        gen_rand_string(4, 4, c.c_zip);

        for (i = 4; i < 9; ++i) 
                c.c_zip[i] = '1';            

        gen_rand_string(16, 16, c.c_phone);

        c.c_middle[0] = 'O';
        c.c_middle[1] = 'E';
        c.c_middle[2] = '\0';

        gen_rand_string(300, 500, c.c_data);
        key = tpcc_util::create_customer_key(wh_id, d_id, c_id);
        if (get_write_ref(key, CUSTOMER_TABLE, (void**)&record) == false)
                return false;
        memcpy(record, &c, sizeof(customer_record));
        return true;
}

uint32_t setup_tpcc::num_writes()
{
        return _high - _low + 1;
}

void setup_tpcc::get_writes(struct big_key *array)
{
        uint32_t i;

        if (_typ == WAREHOUSE) {
                for (i = _low; i <= _high; ++i) {
                        array[i-_low].key = (uint64_t)i;
                        array[i-_low].table_id = WAREHOUSE_TABLE;
                }
        } else if (_typ == DISTRICT) {
                for (i = _low; i <= _high; ++i) {
                        array[i - _low].key = tpcc_util::create_district_key(_wh, i);
                        array[i - _low].table_id = DISTRICT_TABLE;
                }
        } else if (_typ == CUSTOMER) {
                for (i = _low; i <= _high; ++i) {
                        array[i - _low].key = tpcc_util::create_customer_key(_wh, _dstrct, i);
                        array[i - _low].table_id = CUSTOMER_TABLE;
                }
        } else if (_typ == ITEM) {
                for (i = _low; i <= _high; ++i) {
                        array[i - _low].key = (uint64_t)i;
                        array[i - _low].table_id = ITEM_TABLE;
                }
        } else if (_typ == STOCK) {
                for (i = _low; i <= _high; ++i) {
                        array[i - _low].key = tpcc_util::create_stock_key(_wh, i);
                        array[i - _low].table_id = STOCK_TABLE;
                }
        } else {
                assert(false);
        }
}

/* XXX For now, implement the bare minimum required for NewOrder and Payment */
bool setup_tpcc::Run()
{
        uint32_t i;
        bool ret;
        
        ret = true;
        if (_typ == WAREHOUSE) {
                for (i = _low; i <= _high; ++i) 
                        ret = gen_warehouse(i);
        } else if (_typ == DISTRICT) {
                for (i = _low; i <= _high; ++i) 
                        ret = gen_district(_wh, i);
        } else if (_typ == CUSTOMER) {
                for (i = _low; i <= _high; ++i) 
                        ret = gen_customer(_wh, _dstrct, i);
        } else if (_typ == ITEM) {
                for (i = _low; i <= _high; ++i)                        
                        ret = gen_item(i);
        } else if (_typ == STOCK) {
                for (i = _low; i <= _high; ++i) 
                        ret = gen_stock(_wh, i);
        } else {
                assert(false);
        }
        return ret;
}

new_order::new_order(uint32_t warehouse_id, uint32_t district_id, 
                     uint32_t customer_id, 
                     uint32_t num_items,
                     uint64_t *items, 
                     uint32_t *quantities,
                     uint64_t *supplier_warehouses)
{
        uint32_t i, j;
        uint64_t temp[20];
        assert(num_items < 20);

        _warehouse_id = warehouse_id;
        _district_id = district_id;
        _customer_id = customer_id;
        _all_local = true;
        
        for (i = 0; i < num_items; ++i) 
                temp[i] = items[i];

        std::sort(temp, &temp[i]);
        for (i = 0; i < num_items; ++i) {
                
                /* Find the index of the item */
                for (j = 0; j < num_items; ++j) 
                        if (items[j] == temp[i])
                                break;
                
                _items.push_back(items[j]);
                _order_quantities.push_back(quantities[j]);
                _supplier_warehouse_ids.push_back(supplier_warehouses[j]);
        }
}

uint32_t new_order::num_rmws()
{
        /* District and stocks */
        return 1 + _items.size();
}

uint32_t new_order::num_reads()
{
        /* Warehouse, customer, and items */
        return 2 + _items.size();
}

void new_order::get_rmws(struct big_key *array)
{
        uint64_t d_id, stck_id;
        uint32_t i, nitems;

        /* District */
        d_id = tpcc_util::create_district_key(_warehouse_id, _district_id);
        array[0].key = d_id;
        array[0].table_id = DISTRICT_TABLE;
        
        /* Stocks */
        nitems = _items.size();
        for (i = 0; i < nitems; ++i) {
                stck_id = tpcc_util::create_stock_key(_supplier_warehouse_ids[i],
                                                      _items[i]);
                array[1+i].key = stck_id;
                array[1+i].table_id = STOCK_TABLE;
        }
}

void new_order::get_reads(struct big_key *array)
{
        uint64_t c_id;
        uint32_t i, nitems;

        /* Warehouse */
        array[0].key = (uint64_t)_warehouse_id;
        array[0].table_id = WAREHOUSE_TABLE;

        /* Customer */
        c_id = tpcc_util::create_customer_key(_warehouse_id, _district_id, 
                                              _customer_id);
        array[1].key = c_id;
        array[1].table_id = CUSTOMER_TABLE;
        
        /* Item */
        nitems = _items.size();
        for (i = 0; i < nitems; ++i) {
                array[2+i].key = (uint64_t)_items[i];
                array[2+i].table_id = ITEM_TABLE;
        }
}

/* Insert an order record */
bool new_order::insert_oorder(uint32_t order_id, bool all_local)
{
        oorder_record *oorder;
        uint64_t oorder_key;
        
        oorder_key = tpcc_util::create_new_order_key(_warehouse_id, _district_id,
                                                    order_id);
        if (insert_record(oorder_key, OORDER_TABLE, (void**)&oorder) == false)
                return false;
        oorder->o_id = order_id;
        oorder->o_w_id = _warehouse_id;
        oorder->o_d_id = _district_id;
        oorder->o_c_id = _customer_id;
        oorder->o_carrier_id = 0;
        oorder->o_ol_cnt = _items.size();
        oorder->o_all_local = all_local;
        return true;
}

bool new_order::read_warehouse(uint32_t warehouse_id, float *ret)
{
        warehouse_record *wh;
        
        if (get_read_ref((uint64_t)warehouse_id, WAREHOUSE_TABLE, (void**)&wh) == false)
                return false;
        *ret = wh->w_tax;
        return true;
}

/* Increment district's next order id field */
bool new_order::update_district(uint32_t *order_id, float *district_tax)
{
        uint64_t district_key;
        district_record *district;
        
        district_key = tpcc_util::create_district_key(_warehouse_id, 
                                                     _district_id);
        if (get_write_ref(district_key, DISTRICT_TABLE, (void**)&district) == false)
                return false;
        *order_id = district->d_next_o_id;
        *district_tax = district->d_tax;
        district->d_next_o_id += 1;
        return true;
}

/* Read customer discount */
bool new_order::get_customer_discount(float *ret)
{
        uint64_t customer_key;
        customer_record *customer;

        customer_key = tpcc_util::create_customer_key(_warehouse_id, 
                                                     _district_id, 
                                                     _customer_id);
        if (get_read_ref(customer_key, CUSTOMER_TABLE, (void**)&customer) == false)
                return false;
        *ret = customer->c_discount;
        return true;
}

/* Insert a new order record */
bool new_order::insert_new_order(uint32_t order_id)
{
        uint64_t new_order_key;
        new_order_record *record;
        
        new_order_key = tpcc_util::create_new_order_key(_warehouse_id, 
                                                       _district_id, 
                                                       order_id);
        if (insert_record(new_order_key, NEW_ORDER_TABLE, (void**)&record) == false)
                return false;
        record->no_w_id = _warehouse_id;
        record->no_d_id = _district_id;
        record->no_o_id = order_id;
        return true;
}

/* Process one out of the items requested */
bool new_order::process_item(uint32_t item_number, uint32_t order_id, 
                             float w_tax, 
                             float d_tax, 
                             float c_disc)
{
        assert(item_number < _items.size());
        assert(item_number < _order_quantities.size());
        assert(item_number < _supplier_warehouse_ids.size());
        
        uint32_t item_id, order_quantity, supplier_warehouse;
        uint64_t stock_key, order_line_key;
        item_record *item;
        stock_record *stock;
        order_line_record *order_line;
        char *dist_info;
        float item_amount;

        item_id = _items[item_number];
        order_quantity = _order_quantities[item_number];
        supplier_warehouse = _supplier_warehouse_ids[item_number];
        stock_key = tpcc_util::create_stock_key(supplier_warehouse, item_id);
        
        if (get_read_ref((uint64_t)item_id, ITEM_TABLE, (void**)&item) == false)
                return false;
        if (get_write_ref(stock_key, STOCK_TABLE, (void**)&stock) == false)
                return false;
        
        /* Update inventory */
        if (stock->s_order_cnt - order_quantity >= 10) 
                stock->s_quantity -= order_quantity;
        else 
                stock->s_quantity += -order_quantity + 91;
        
        if (supplier_warehouse != _warehouse_id) 
                stock->s_remote_cnt += 1;
        
        stock->s_ytd += order_quantity;
        switch (_district_id) {
        case 0:
                dist_info = stock->s_dist_01;
                break;
        case 1:
                dist_info = stock->s_dist_02;
                break;
        case 2:
                dist_info = stock->s_dist_03;
                break;
        case 3:
                dist_info = stock->s_dist_04;
                break;
        case 4:
                dist_info = stock->s_dist_05;
                break;
        case 5:
                dist_info = stock->s_dist_06;
                break;
        case 6:
                dist_info = stock->s_dist_07;
                break;
        case 7:
                dist_info = stock->s_dist_08;
                break;
        case 8:
                dist_info = stock->s_dist_09;
                break;
        case 9:
                dist_info = stock->s_dist_10;
                break;
        default:
                assert(false);
        }

        item_amount = item->i_price*(1+w_tax+d_tax)*(1-c_disc);
        item_amount *= _order_quantities[item_number];
        order_line_key = tpcc_util::create_order_line_key(_warehouse_id, 
                                                         _district_id, 
                                                         order_id, 
                                                         item_number);
        /*
        if (insert_record(order_line_key, ORDER_LINE_TABLE, (void**)&order_line) == false)
                return false;
        order_line->ol_o_id = order_id;
        order_line->ol_d_id = _district_id;
        order_line->ol_w_id = _warehouse_id;
        order_line->ol_number = item_number;
        order_line->ol_supply_w_id = _supplier_warehouse_ids[item_number];
        order_line->ol_quantity = _order_quantities[item_number];
        order_line->ol_amount = _order_quantities[item_number]*item->i_price;
        //        memcpy(order_line->ol_dist_info, dist_info, sizeof(char)*25);
        */
        return true;
}

bool new_order::Run()
{
        float district_tax, warehouse_tax, customer_discount;
        uint32_t order_id, i, num_items;
        
        if (read_warehouse(_warehouse_id, &warehouse_tax) == false)
                return false;
        if (update_district(&order_id, &district_tax) == false)
                return false;
        if (get_customer_discount(&customer_discount) == false)
                return false;

        if (insert_new_order(order_id) == false)
                return false;
        num_items = _items.size();
        for (i = 0; i < num_items; ++i) 
                if (process_item(i, order_id, warehouse_tax, district_tax, 
                                 customer_discount) == false)
                        return false;
        /*
        if (insert_oorder(order_id, _all_local) == false)
                return false;        
        */
        return true;
}

payment::payment(uint32_t warehouse_id, uint32_t district_id, 
                 uint32_t customer_id, 
                 uint32_t customer_warehouse_id,
                 uint32_t customer_district_id,
                 float h_amount,
                 uint32_t time)
{
        _warehouse_id = warehouse_id;
        _district_id = district_id;
        _customer_id = customer_id;
        _customer_warehouse_id = customer_warehouse_id;
        _customer_district_id = customer_district_id;
        _h_amount = h_amount;
        _time = time;
}

uint32_t payment::num_rmws()
{
        return 3;
}

void payment::get_rmws(struct big_key *array)
{
        uint64_t d_id, c_id;

        /* Warehouse */
        array[0].key = (uint64_t)_warehouse_id;
        array[0].table_id = WAREHOUSE_TABLE;
        
        /* District */
        d_id = tpcc_util::create_district_key(_warehouse_id, _district_id);
        array[1].key = d_id;
        array[1].table_id = DISTRICT_TABLE;
        
        /* Customer */
        c_id = tpcc_util::create_customer_key(_customer_warehouse_id, 
                                              _customer_district_id,
                                              _customer_id);
        array[2].key = c_id;
        array[2].table_id = CUSTOMER_TABLE;
}

/* Insert history record */
bool payment::insert_history(__attribute__((unused)) char *warehouse_name, __attribute__((unused)) char *district_name)
{ 
        uint64_t history_key;
        history_record *hist;
        //        static const char *empty = "    ";
        //        const char *holder[3] = {warehouse_name, empty, district_name};
 
        history_key = guid();
        if (insert_record(history_key, HISTORY_TABLE, (void**)&hist) == false)
                return false;
        hist->h_c_id = _customer_id;
        hist->h_c_d_id = _customer_district_id;
        hist->h_c_w_id = _customer_warehouse_id;
        hist->h_d_id = _district_id;
        hist->h_w_id = _warehouse_id;
        hist->h_date = _time;
        hist->h_amount = _h_amount;
        //        tpcc_util::append_strings(hist->h_data, holder, 26, 3);
        return true;
}
 
/* Update the warehouse table */
bool payment::warehouse_update(char **ret)
{ 
        assert(_warehouse_id < tpcc_config::num_warehouses);
        warehouse_record *warehouse;        
        
        if (get_write_ref((uint64_t)_warehouse_id, WAREHOUSE_TABLE, (void**)&warehouse) == false)
                return false;
        warehouse->w_ytd += _h_amount;
        *ret = warehouse->w_name;
        return true;
}
 
/* Update the district table */
bool payment::district_update(char **ret)
{ 
        assert(_district_id < 10);
        uint64_t d_id;
        district_record *district;
        
        d_id = tpcc_util::create_district_key(_warehouse_id, _district_id);
        if (get_write_ref(d_id, DISTRICT_TABLE, (void**)&district) == false)
                return false;
        district->d_ytd += _h_amount;
        *ret = district->d_name;
        return true;
} 
 
/* Update the customer table */
bool payment::customer_update()
{ 
        uint64_t customer_key;
        customer_record *cust;
        static const char *credit = "BC";
        static const char *space = " ";
        char c_id_str[17], c_d_id_str[17], c_w_id_str[17], d_id_str[17],
                w_id_str[17], h_amount_str[17];
        
        customer_key = tpcc_util::create_customer_key(_customer_warehouse_id, 
                                                     _customer_district_id,
                                                     _customer_id);

        if (get_write_ref(customer_key, CUSTOMER_TABLE, (void**)&cust) == false)
                return false;
        if (strcmp(credit, cust->c_credit) == 0) {
                sprintf(c_id_str, "%x", _customer_id);
                sprintf(c_d_id_str, "%x", _customer_district_id);
                sprintf(c_w_id_str, "%x", _customer_warehouse_id);
                sprintf(d_id_str, "%x", _warehouse_id);
                sprintf(w_id_str, "%x", _district_id);
                sprintf(h_amount_str, "%lx", (uint64_t)_h_amount);
                
                static const char *holder[11] = {c_id_str, space, c_d_id_str, 
                                                 space, c_w_id_str, space, 
                                                 d_id_str, space, w_id_str, 
                                                 space, h_amount_str};
                tpcc_util::append_strings(cust->c_data, holder, 501, 11);
        } else {
                cust->c_balance -= _h_amount;
                cust->c_ytd_payment += _h_amount;
                cust->c_payment_cnt += 1;
        }
        return true;
}
 
/* Run payment txn  */
bool payment::Run()
{ 
        char *warehouse_name, *district_name;
        bool success;

        success = warehouse_update(&warehouse_name);
        if (success == false)
                return false;
        success = district_update(&district_name);
        if (success == false)
                return false;
        success = customer_update();
        if (success == false)
                return false;
        success = insert_history(warehouse_name, district_name);
        if (success == false)
                return false;
        return true;
} 

 
// stock_level::stock_level(uint32_t warehouse_id, uint32_t district_id)
// {
//         _warehouse_id = warehouse_id;
//         _district_id = district_id;
//         _order_id = 0;
//         _num_stocks = 0;
//         _stock_reads = 0;
//         _stocks = (uint32_t*)zmalloc(sizeof(uint32_t)*NUM_STOCK_LEVEL_ORDERS*20);
// }
//  
// * 
// * Obtain the set of order whose constituent items's stock levels we're going to
// * read 
// */
// 
// oid stock_level::read_district()
// 
//        uint64_t district_key;
//        district_record *district;
//        
//        district_key = tpcc_util::create_district_key(_warehouse_id, 
//                                                      _district_id);
//        district = (district_record*)get_read_ref(district_key, DISTRICT_TABLE);
//        _order_id = district->d_next_o_id;
//  
// 
// * Track the item corresponding to the given order line */
// oid stock_level::read_single_order_line(uint64_t order_line_key)
// 
//        order_line_record *order_line;
//        uint32_t i;
// 
//        order_line = (order_line_record*)get_read_ref(order_line_key, 
//                                                      ORDER_LINE_TABLE);
//        for (i = 0; i < _num_stocks; ++i) {
//                if (_stocks[i] == order_line->ol_i_id)
//                        break;
//        }
//        if (i == _num_stocks) {
//                _stocks[i] = order_line->ol_i_id;
//                _num_stocks += 1;
//        }
//  
// 
// * Read order line records in order to obtain stock ids */
// oid stock_level::read_order_lines()
// 
//        uint64_t oorder_key, order_line_key;
//        uint32_t i, j, order_line_counts[NUM_STOCK_LEVEL_ORDERS];
//        oorder_record *oorder;
//        
//        for (i = 0; i < NUM_STOCK_LEVEL_ORDERS; ++i) {
//                oorder_key = tpcc_util::create_new_order_key(_warehouse_id, 
//                                                             _district_id, 
//                                                             _order_id);
//                oorder = (oorder_record*)get_read_ref(oorder_key, OORDER_TABLE);
//                order_line_counts[i] = oorder->o_ol_cnt;
//        }
//        for (i = 0; i < NUM_STOCK_LEVEL_ORDERS; ++i) {
//                for (j = 0; j < order_line_counts[i]; ++j) {
//                        order_line_key = 
//                                tpcc_util::create_order_line_key(_warehouse_id, 
//                                                                 _district_id, 
//                                                                 _order_id - i,
//                                                                 j);
//                        read_single_order_line(order_line_key);
//                }
//        }
//  
// 
// * For every item, read the quantity of stock left */
// oid stock_level::read_stock()
//  
//        uint32_t i, total;
//        stock_record *stock;
//        uint64_t stock_key;
// 
//        total = 0;
//        for (i = 0; i < _num_stocks; ++i) {
//                stock_key = tpcc_util::create_stock_key(_warehouse_id, 
//                                                        _stocks[i]);
//                stock = (stock_record*)get_read_ref(stock_key, STOCK_TABLE);
//                if (stock->s_quantity - _threshold > 0)
//                        total += 1;
//        }
//        _stock_reads = total;                
//  
// 
// ool stock_level::Run()
//  
//        read_district();
//        read_order_lines();
//        read_stock();
//        return true;
//  
// 
// rder_status::order_status(uint32_t warehouse_id, uint32_t district_id, 
//                           uint32_t customer_id, 
//                           uint64_t customer_name_idx, 
//                           bool use_name)
//  
//        _warehouse_id = warehouse_id;
//        _district_id = district_id;
//        _customer_id = customer_id;
//        _customer_name_idx = customer_name_idx;
//        _use_name = use_name;
// 
// 
// * Get the details of the order we're interested in */
// oid order_status::read_open_order_index()
// 
//        uint64_t customer_key;
// 
//        customer_key = tpcc_util::create_customer_key(_warehouse_id, 
//                                                      _district_id,
//                                                      _customer_id);
//        _order_key = *(uint64_t*)get_read_ref(customer_key, CUSTOMER_ORDER_INDEX);
// 
// 
// * Read the open order record to obtain the order-line records to read. */
// oid order_status::read_open_order()
// 
//        oorder_record *open_order;
// 
//        open_order = (oorder_record*)get_read_ref(_order_key, OORDER_TABLE);
//        _num_order_lines = open_order->o_ol_cnt;
// 
// 
// * Read order-line records, sum up quantity we need */
// oid order_status::read_order_lines()
// 
//        uint32_t i, quantity, order_id;
//        uint64_t order_line_key;
//        order_line_record *order_line;
//        
//        order_id = tpcc_util::get_order_key(_order_key);
//        quantity = 0;
//        for (i = 0; i < _num_order_lines; ++i) {
//                order_line_key = tpcc_util::create_order_line_key(_warehouse_id,
//                                                                  _district_id, 
//                                                                  order_id, 
//                                                                  i);
//                order_line = (order_line_record*)get_read_ref(order_line_key, ORDER_LINE_TABLE);
//                quantity = order_line->ol_quantity;
//        }
//        _order_line_quantity = quantity;
// 
// 
// * Order Status transaction */
// ool order_status::Run()
// 
//        read_open_order_index();
//        read_open_order();
//        read_order_lines();
//        return true;
// 
// 
// elivery::delivery(uint32_t warehouse_id)
// 
//        _warehouse_id = warehouse_id;
// 
// 
// oid delivery::read_next_delivery()
//  
//        uint64_t district_key, *last_delivered_ptr;
//        district_record *district;
//        uint32_t i;
// 
//        for (i = 0; i < NUM_DISTRICTS; ++i) {
//                district_key = tpcc_util::create_district_key(_warehouse_id, 
//                                                              i);
//                last_delivered_ptr = (uint64_t*)get_read_ref(district_key, 
//                                                             DELIVERY_TABLE);
//                district = (district_record*)get_write_ref(district_key, 
//                                                          DISTRICT_TABLE);
//                if (district->d_next_o_id > *last_delivered_ptr) {
//                        _to_deliver[i] = true;
//                        _delivery_order_id[i] = *last_delivered_ptr + 1;
//                        *last_delivered_ptr += 1;
//                }
//        }
// 
// 
// oid delivery::remove_new_orders()
//  
//        uint32_t i;
//        uint64_t order_id;
//        
//        for (i = 0; i < 10; ++i) {
//                if (_to_deliver[i] == true) {
//                        order_id = 
//                                tpcc_util::create_new_order_key(_warehouse_id, 
//                                                                i,
//                                                                _delivery_order_id[i]);
//                        remove_record(order_id, NEW_ORDER_TABLE);
//                }
//        }
// 
// 
// oid delivery::read_orders()
// 
//        uint32_t i, j;
//        uint64_t oorder_key, order_line_key;
//        oorder_record *oorders[NUM_DISTRICTS];
//        order_line_record *order_line;
// 
//        for (i = 0; i < NUM_DISTRICTS; ++i) {
//                if (_to_deliver[i] == false) {
//                        oorders[i] = NULL;
//                        continue;
//                }
//                oorder_key = tpcc_util::create_new_order_key(_warehouse_id, i, 
//                                                             _delivery_order_id[i]);
//                oorders[i] = (oorder_record*)get_read_ref(oorder_key, 
//                                                          OORDER_TABLE);
//                _customers[i] = oorders[i]->o_c_id;
//        }
//        
//        for (i = 0; i < NUM_DISTRICTS; ++i) {
//                _amounts[i] = 0;
//                for (j = 0; j < oorders[i]->o_ol_cnt; ++i) {
//                        order_line_key = 
//                                tpcc_util::create_order_line_key(_warehouse_id, 
//                                                                 i,
//                                                                 oorders[i]->o_id,
//                                                                 j);
//                        order_line = (order_line_record*)get_read_ref(order_line_key,
//                                                                      ORDER_LINE_TABLE);
//                        _amounts[i] += order_line->ol_amount;
//                }
//        }
// 
// 
// oid delivery::update_customer()
// 
//        uint32_t i;
//        uint64_t customer_key;
//        customer_record *customer;
// 
//        for (i = 0; i < NUM_DISTRICTS; ++i) {
//                if (_to_deliver[i] == false)
//                        continue;
//                customer_key = tpcc_util::create_customer_key(_warehouse_id, i, 
//                                                   _customers[i]);
//                customer = (customer_record*)get_write_ref(customer_key, 
//                                                           CUSTOMER_TABLE);
//                customer->c_balance += _amounts[i];
//                customer->c_delivery_cnt += 1;
//        }
// 
// 
// ool delivery::Run()
// 
//        read_next_delivery();        
//        read_orders();
//        remove_new_orders();
//        update_customer();
//        return true;
// 
