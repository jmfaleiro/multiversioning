#include <pipelined_tpcc.h>
#include <algorithm>
#include <cpuinfo.h>

p_new_order::warehouse_read::warehouse_read(uint32_t wh)
{
        _wh = wh;
}

uint32_t p_new_order::warehouse_read::num_reads()
{
        return 1;
}

void p_new_order::warehouse_read::get_reads(big_key *array)
{
        array[0].key = (uint64_t)_wh;
        array[0].table_id = WAREHOUSE_TABLE;
}

bool p_new_order::warehouse_read::Run()
{
        warehouse_record *wh_rec;
        
        wh_rec = (warehouse_record*)get_read_ref((uint64_t)_wh, WAREHOUSE_TABLE);
       _wh_tax = wh_rec->w_tax;
       return true;
}

float p_new_order::warehouse_read::get_tax()
{
        return _wh_tax;
}

p_new_order::district_update::district_update(uint32_t wh, uint32_t d)
{
        _wh = wh;
        _d = d;
}

uint32_t p_new_order::district_update::num_rmws()
{
        return 1;
}

void p_new_order::district_update::get_rmws(big_key *array)
{
        array[0].key = tpcc_util::create_district_key(_wh, _d);
        array[0].table_id = DISTRICT_TABLE;        
}

bool p_new_order::district_update::Run()
{
        uint64_t d_key;
        district_record *district;
        
        d_key = tpcc_util::create_district_key(_wh, _d);
        district = (district_record*)get_write_ref(d_key, DISTRICT_TABLE);
        _next_o_id = district->d_next_o_id;        
        _d_tax = district->d_tax;
        district->d_next_o_id += 1;
        return true;
}

float p_new_order::district_update::get_tax()
{
        return _d_tax;
}

uint32_t p_new_order::district_update::get_order_id()
{
        return _next_o_id;
}

p_new_order::customer_read::customer_read(uint32_t wh, uint32_t d, uint32_t c)
{
        _wh = wh;
        _d = d;
        _c = c;
}

uint32_t p_new_order::customer_read::num_reads()
{
        return 1;
}

void p_new_order::customer_read::get_reads(big_key *array)
{
        array[0].key = tpcc_util::create_customer_key(_wh, _d, _c);
        array[0].table_id = CUSTOMER_TABLE;
}

bool p_new_order::customer_read::Run()
{
        customer_record *customer;
        uint64_t customer_key;
        
        customer_key = tpcc_util::create_customer_key(_wh, _d, _c);
        customer = (customer_record*)get_read_ref(customer_key, CUSTOMER_TABLE);
        _c_discount = customer->c_discount;        
        return true;
}

float p_new_order::customer_read::get_discount()
{
        return _c_discount;
}

p_new_order::new_order_ins::new_order_ins(uint32_t wh, uint32_t d, 
                                          district_update *dist_txn)
{
        _wh = wh;
        _d = d;
        _dist_txn = dist_txn;
}

bool p_new_order::new_order_ins::Run()
{
        uint64_t new_order_key;
        new_order_record *record;
        uint32_t order_id;
        
        order_id = _dist_txn->get_order_id();
        new_order_key = tpcc_util::create_new_order_key(_wh, _d, order_id);
        record = (new_order_record*)insert_record(new_order_key, NEW_ORDER_TABLE);
        record->no_w_id = _wh;
        record->no_d_id = _d;
        record->no_o_id = order_id;
        return true;
}

p_new_order::oorder_ins::oorder_ins(uint32_t wh, uint32_t d, uint32_t c, 
                                    uint32_t nitems, 
                                    bool all_local, 
                                    district_update *dist_txn)
{
        _wh = wh;
        _d = d;
        _c = c;
        _nitems = nitems;
        _all_local = all_local;
        _dist_txn = dist_txn;
}

bool p_new_order::oorder_ins::Run()
{
        oorder_record *oorder;
        uint64_t oorder_key;
        uint32_t order_id;
        
        order_id = _dist_txn->get_order_id();

        oorder_key = tpcc_util::create_new_order_key(_wh, _d, order_id);
        oorder = (oorder_record*)insert_record(oorder_key, OORDER_TABLE);
        oorder->o_id = order_id;
        oorder->o_w_id = _wh;
        oorder->o_d_id = _d;
        oorder->o_c_id = _c;
        oorder->o_carrier_id = 0;
        oorder->o_ol_cnt = _nitems;
        oorder->o_all_local = _all_local;
        return true;
}

p_new_order::process_items::process_items(uint32_t wh, uint32_t d, 
                                          uint64_t *supplier_whs, 
                                          uint64_t *items, 
                                          uint32_t *order_quantities,
                                          uint32_t nitems,
                                          warehouse_read *wh_txn,
                                          district_update *d_txn,
                                          customer_read *c_txn)
{
        _wh = wh;
        _d = d;
        _supplier_whs = supplier_whs;
        _items = items;
        _order_quantities = order_quantities;
        _nitems = nitems;
        _wh_txn = wh_txn;
        _d_txn = d_txn;
        _c_txn = c_txn;
}

bool p_new_order::process_items::Run()
{
        uint32_t i, order_id;
        float w_tax, d_tax, c_disc;

        order_id = _d_txn->get_order_id();
        w_tax = _wh_txn->get_tax();
        d_tax = _d_txn->get_tax();
        c_disc = _c_txn->get_discount();

        for (i = 0; i < _nitems; ++i) 
                process_single(i, order_id, w_tax, d_tax, c_disc);
        return true;
}

uint32_t p_new_order::process_items::num_rmws()
{
        return _nitems;
}

void p_new_order::process_items::get_rmws(big_key *array)
{
        uint32_t i;
        uint64_t stock_key;

        for (i = 0; i < _nitems; ++i) {
                stock_key = tpcc_util::create_stock_key(_supplier_whs[i], _items[i]);
                array[i].key = stock_key;
                array[i].table_id = STOCK_TABLE;
        }
}

void p_new_order::process_items::process_single(uint32_t n, uint32_t order_id, 
                                               float w_tax, 
                                               float d_tax, 
                                               float c_disc)
{
        assert(n < _nitems);

        uint32_t item_id, order_quantity, supplier_warehouse;
        uint64_t stock_key, order_line_key;
        item_record *item;
        stock_record *stock;
        order_line_record *order_line;
        char *dist_info;
        float item_amount;

        item_id = _items[n];
        order_quantity = _order_quantities[n];
        supplier_warehouse = _supplier_whs[n];
        stock_key = tpcc_util::create_stock_key(supplier_warehouse, item_id);
        
        item = (item_record*)get_read_ref((uint64_t)item_id, ITEM_TABLE);
        stock = (stock_record*)get_write_ref(stock_key, STOCK_TABLE);
        
        /* Update inventory */
        if (stock->s_order_cnt - order_quantity >= 10) 
                stock->s_quantity -= order_quantity;
        else 
                stock->s_quantity += -order_quantity + 91;
        
        if (supplier_warehouse != _wh) 
                stock->s_remote_cnt += 1;
        
        stock->s_ytd += order_quantity;
        switch (_d) {
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
        item_amount *= _order_quantities[n];
        order_line_key = tpcc_util::create_order_line_key(_wh, _d, order_id, n);
        order_line = (order_line_record*)insert_record(order_line_key, ORDER_LINE_TABLE);
        order_line->ol_o_id = order_id;
        order_line->ol_d_id = _d;
        order_line->ol_w_id = _wh;
        order_line->ol_number = n;
        order_line->ol_supply_w_id = _supplier_whs[n];
        order_line->ol_quantity = _order_quantities[n];
        order_line->ol_amount = _order_quantities[n]*item->i_price;
        strcpy(order_line->ol_dist_info, dist_info);
}

p_payment::warehouse_update::warehouse_update(uint32_t wh, float h_amount)
{
        _wh = wh;
        _h_amount = h_amount;
}

uint32_t p_payment::warehouse_update::num_rmws()
{
        return 1;
}

void p_payment::warehouse_update::get_rmws(big_key *array)
{
        array[0].key = (uint64_t)_wh;
        array[0].table_id = WAREHOUSE_TABLE;
}

bool p_payment::warehouse_update::Run()
{
        assert(_wh < tpcc_config::num_warehouses);        
        warehouse_record *warehouse;
        
        warehouse = (warehouse_record*)get_write_ref((uint64_t)_wh, WAREHOUSE_TABLE);
        warehouse->w_ytd += _h_amount;
        _wh_name = warehouse->w_name;
        return true;
}

char* p_payment::warehouse_update::get_wh_name()
{
        return _wh_name;
}

p_payment::district_update::district_update(uint32_t wh, uint32_t d, float h_amount)
{
        _wh = wh;
        _d = d;
        _h_amount = h_amount;
}

uint32_t p_payment::district_update::num_rmws()
{
        return 1;
}

void p_payment::district_update::get_rmws(big_key *array)
{
        uint64_t dist_key;
        
        array[0].key = tpcc_util::create_district_key(_wh, _d);
        array[0].table_id = DISTRICT_TABLE;
}

bool p_payment::district_update::Run()
{
        assert(_wh < tpcc_config::num_warehouses);
        assert(_d < NUM_DISTRICTS);
        uint64_t district_key;
        district_record *district;
        
        district_key = tpcc_util::create_district_key(_wh, _d);
        district = (district_record*)get_write_ref(district_key, DISTRICT_TABLE);
        district->d_ytd += _h_amount;
        _d_name = district->d_name;
        return true;
}

char* p_payment::district_update::get_d_name()
{
        return _d_name;
}

p_payment::customer_update::customer_update(uint32_t wh, uint32_t d, 
                                            uint32_t c_wh, 
                                            uint32_t c_d, 
                                            uint32_t c, 
                                            float h_amount)
{
        _wh = wh;
        _d = d;
        _c_wh = c_wh;
        _c_d = c_d;
        _c = c;
        _h_amount = h_amount;
}

uint32_t p_payment::customer_update::num_rmws()
{
        return 1;
}

void p_payment::customer_update::get_rmws(big_key *array)
{
        array[0].key = tpcc_util::create_customer_key(_c_wh, _c_d, _c);
        array[0].table_id = CUSTOMER_TABLE;
}

bool p_payment::customer_update::Run()
{
        assert(_c_wh < tpcc_config::num_warehouses);
        assert(_c_d < NUM_DISTRICTS);
        assert(_c < NUM_CUSTOMERS);
        
        uint64_t customer_key;
        customer_record *cust;
        static const char *credit = "BC";
        static const char *space = " ";
        char c_id_str[17], c_d_id_str[17], c_w_id_str[17], d_id_str[17],
                w_id_str[17], h_amount_str[17];
        
        customer_key = tpcc_util::create_customer_key(_c_wh, _c_d, _c);

        cust = (customer_record*)get_write_ref(customer_key, CUSTOMER_TABLE);
        if (strcmp(credit, cust->c_credit) == 0) {
                sprintf(c_id_str, "%x", _c);
                sprintf(c_d_id_str, "%x", _c_d);
                sprintf(c_w_id_str, "%x", _c_wh);
                sprintf(d_id_str, "%x", _wh);
                sprintf(w_id_str, "%x", _d);
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

p_payment::history_ins::history_ins(uint32_t wh, uint32_t d, uint32_t c_wh, 
                                    uint32_t c_d,
                                    uint32_t c, 
                                    uint32_t time,
                                    float h_amount,
                                    warehouse_update *wh_txn,
                                    district_update *d_txn)
{
        _wh = wh;
        _d = d;
        _c_wh = c_wh;
        _c_d = c_d;
        _c = c;
        _wh_txn = wh_txn;
        _d_txn = d_txn;
        _h_amount = h_amount;
        _time = time;
}

bool p_payment::history_ins::Run()
{
        uint64_t history_key;
        history_record *hist;
        static const char *empty = "    ";
        char *warehouse_name, *district_name;        
 
        warehouse_name = _wh_txn->get_wh_name();
        district_name = _d_txn->get_d_name();

        const char *holder[3] = {warehouse_name, empty, district_name};

        history_key = tpcc_util::create_history_key(_wh, _d, guid());

        hist = (history_record*)insert_record(history_key, HISTORY_TABLE);
        hist->h_c_id = _c;
        hist->h_c_d_id = _c_d;
        hist->h_c_w_id = _c_wh;
        hist->h_d_id = _d;
        hist->h_w_id = _wh;
        hist->h_date = _time;
        hist->h_amount = _h_amount;
        tpcc_util::append_strings(hist->h_data, holder, 26, 3);
        return true;
}
