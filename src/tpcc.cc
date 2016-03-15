
void new_order::insert_open_order(uint32_t order_id, bool all_local)
{
        oorder_record oorder;
        uint64_t oorder_key;
        
        oorder_key = create_new_order_key(_warehouse_id, _district_id, 
                                          order_id);
        oorder.o_id = order_id;
        oorder.o_w_id = _warehouse_id;
        oorder.o_d_id = _district_id;
        oorder.o_c_id = _customer_id;
        oorder.o_carrier_id = 0;
        oorder.o_ol_cnt = _items.size();
        oorder.o_all_local = all_local;        
        insert_record(oorder_key, OORDER_TABLE, &oorder);
}

void new_order::update_district(uint32_t *order_id, float *district_tax)
{
        uint64_t district_key;
        district_record *district;
        
        district_key = create_district_key(_warehouse_id, _district_id);
        district = get_write_ref(district_key, DISTRICT_TABLE);
        *order_id = district->d_next_o_id;
        *district_tax = district->d_tax;
        district->d_next_o_id += 1;
}

float new_order::get_customer_discount()
{
        uint64_t customer_key;
        customer_record *customer;

        customer_key = create_customer_key(_warehouse_id, _district_id, 
                                           _customer_id);
        customer = get_read_ref(customer_key, CUSTOMER_TABLE);
        return customer->c_discount;
}

void new_order::insert_new_order(__attribute__((unused)) uint32_t order_id)
{
        assert(false);
}

void new_order::process_item(uint32_t item_number, uint32_t order_id)
{
        assert(item_number < _items.size());
        assert(item_number < _order_quantities.size());
        assert(item_number < _supplier_warehouse_ids.size());
        
        uint32_t item_id, order_quantity, supplier_warehouse;
        uint64_t stock_key, item_key, order_line_key;
        item_record *item;
        stock_record *stock;
        order_line_record order_line;
        char *dist_info;

        item_id = _items[item_number];
        order_quantity = _order_quantities[item_number];
        supplier_warehouse = _supplier_warehouse_ids[item_number];
        stock_key = create_stock_key(_warehouse_id, item_id);
        
        item = get_read_ref((uint64_t)item_id, ITEM_TABLE);
        stock = get_write_ref(stock_key, STOCK_TABLE);
        
        /* Update inventory */
        if (stock->s_order_cnt - order_quantity >= 10) 
                stock->s_quantitiy -= order_quantity;
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

        order_line.ol_o_id = order_id;
        order_line.ol_d_id = _district_id;
        order_line.ol_w_id = _warehouse_id;
        order_line.ol_number = item_number;
        order_line.ol_supply_w_id = _supplier_warehouse_ids[item_number];
        order_line.ol_quantitiy = _order_quantities[item_number];
        order_line.ol_amount = _order_quantities[item_number]*item->i_price;
        order_line_key = create_order_line_key(_warehouse_id, _district_id, 
                                               order_id, item_number);
        insert_record(order_line_key, ORDER_LINE_TABLE, &order_line);
}

bool new_order::Run()
{
        float district_tax, customer_discount, total;
        uint32_t order_id, i, num_items;

        update_district(&order_id, &district_tax);
        customer_discount = get_customer_discount();
        insert_new_order(order_id);
        total = 0;
        for (i = 0; i < num_items; ++i) 
                insert_item(i, order_id);
        insert_oorder();        
        return true;
}

void payment::insert_history()
{
        hist.h_c_id = _customer_id;
        hist.h_c_d_id = _customer_district_id;
        hist.h_c_w_id = _customer_warehouse_id;
        hist.h_d_id = _district_id;
        hist.h_w_id = _warehouse_id;
        hist.h_date = _time;
        hist.h_amount = _amount;
}

char* payment::warehouse_update()
{
        warehouse_record *warehouse;        
        warehouse = get_write_ref(_warehouse_id, WAREHOUSE_TABLE);
        warehouse->w_ytd += _h_amount;
        return warehouse->w_name;
}

char* payment::district_update()
{
        district_record *district;
        district = get_write_ref(_district_id, DISTRICT_TABLE);
        district->d_ytd += _h_amount;
        return district->d_name;
}

void payment::customer_update()
{
        customer_record *cust;
        static const char *credit = "BC";
        static const char *empty = "    ";
        const char *wh_name, *d_name, *holder[3];
        history_record hist;

        cust = get_write_ref(_customer_id, CUSTOMER_TABLE);
        if (strcmp(credit, cust->c_credit) == 0) {
                
        } else {
                cust->c_balance -= _h_amount;
                cust->c_ytd_payment += _h_amount;
                cust->c_payment_cnt += 1;
        }
        
}

bool payment::Run()
{
        _warehouse_name = warehouse_update();
        _district_name = district_update();
        customer_update();
        insert_history();
        return true;
}

stock_level::stock_level(uint32_t warehouse_id, uint32_t district_id)
{
        _warehouse_id = warehouse_id;
        _district_id = district_id;
        _order_id = 0;
        _num_stocks = 0;
        _stock_reads = 0;
        _stocks = zmalloc(sizeof(uint32_t)*NUM_STOCK_LEVEL_ORDERS*20);
}

/* 
 * Obtain the set of order whose constituent items's stock levels we're going to
 * read 
 */
void stock_level::read_district()
{
        uint64_t district_key;
        district_record *district;
        
        district_key = create_district_key(_warehouse_id, _district_id);
        district = (district_record*)get_read_ref(district_key, DISTRICT_TABLE);
        _order_id = district->d_next_o_id;
}

/* Track the item corresponding to the given order line */
void stock_level::read_single_order_line(uint64_t order_line_key)
{
        order_line_record *order_line;
        uint32_t i, item_id;

        order_line = (order_line_record*)get_read_ref(order_line_key, 
                                                      ORDER_LINE_TABLE);
        for (i = 0; i < _num_stocks; ++i) {
                if (_stocks[i] == order_line->ol_i_id)
                        break;
        }
        if (i == _num_stocks) {
                _stocks[i] = order_line->ol_i_id;
                _num_stocks += 1;
        }
}

/* Read order line records in order to obtain stock ids */
void stock_level::read_order_lines()
{
        uint64_t oorder_key, order_line_key;
        uint32_t i, j, order_line_counts[NUM_STOCK_LEVEL_ORDERS];
        oorder_record *oorder;
        order_record *order_line;
        
        for (i = 0; i < NUM_STOCK_LEVEL_ORDERS; ++i) {
                oorder_key = create_new_order_key(_warehouse_id, _district_id, 
                                                  _order_id);
                oorder = (oorder_record*)get_read_ref(oorder_key, OORDER_TABLE);
                order_line_counts[i] = oorder->o_ol_cnt;
        }
        for (i = 0; i < NUM_STOCK_LEVEL_ORDERS; ++i) {
                for (j = 0; j < 0; ++j) {
                        order_line_key = create_order_line_key(_warehouse_id, 
                                                               _district_id, 
                                                               _order_id - i,
                                                               j);
                        read_single_order_line(order_line_key);
                }
        }
}

/* For every item, read the quantity of stock left */
void stock_level::read_stock()
{
        uint32_t i, total;
        stock_record *stock;
        uint64_t stock_key;

        total = 0;
        for (i = 0; i < _num_stocks; ++i) {
                stock_key = create_stock_key(_warehouse_id, _district_id, 
                                             _stocks[i]);
                stock = (stock_record*)get_read_ref(stock_key, STOCK_TABLE);
                if (stock->s_quantity - _threshold > 0)
                        total += 1;
        }
        _stock_reads = total;                
}

bool stock_level::Run()
{
        read_district();
        read_order_lines();
        read_stock();
        return true;
}

order_status::order_status(uint32_t warehouse_id, uint32_t district_id)
{
        _warehouse_id = 0;
        _district_id = 0;
        _order_id = 0;
        _num_order_lines = 0;
}

/* Get the details of the order we're interested in */
void order_status::read_open_order_index()
{
        uint64_t district_key;
        uint64_ *record;

        district_key = create_district_key(_warehouse_id, _district_id);
        record = (uint64_t*)get_read_ref(district_key, OPEN_ORDER_INDEX);
        _order_id = *record;
}

/* Read the open order record to obtain the order-line records to read. */
void order_status::read_open_order()
{
        uint64_t oorder_key;
        uint32_t num_items, i;
        oorder_record *open_order;

        
        oorder_key = create_new_order_key(_warehouse_id, _district_id, 
                                          _order_id);
        open_order = (oorder_record*)get_read_ref(oorder_key, OORDER_TABLE);
        _num_order_lines = open_order->o_ol_cnt;
}

/* Read order-line records, sum up quantity we need */
void order_status::read_order_lines()
{
        uint32_t i, quantity;
        uint64_t order_line_key;
        order_line_record *order_line;
        
        quantity = 0;
        for (i = 0; i < _num_order_lines; ++i) {
                order_line_key = create_order_line_key(_warehouse_id, 
                                                       _district_id, 
                                                       _order_id, 
                                                       i);
                order_line = get_read_ref(order_line_key, ORDER_LINE_TABLE);
                quantity = order_line->ol_quantity;
        }
        _order_line_quantity = quantity;
}

/* Order Status transaction */
bool order_status::Run()
{
        read_open_order_index();
        read_open_order();
        read_order_lines();
        return true;
}

void delivery::read_next_delivery()
{
        uint64_t district_key, *last_delivered_ptr;
        district_record *district;
        uint32_t last_delivered;

        for (i = 1; i <= 10; ++i) {
                district_key = create_district_key(_warehouse_id, _district_id);
                last_delivered_ptr = (uint64_t*)get_read_ref(district_key, 
                                                             LAST_DELIVERED_TABLE);
                district = (district_record*)get_write_ref(district_key, 
                                                          DISTRICT_TABLE);
                if (district->d_next_o_id > *last_delivered_ptr) {
                        _to_deliver[i] = true;
                        _delivery_order_id[i] = *last_delivered_ptr + 1;
                        *last_delivered_ptr += 1;
                }
        }
}

void delivery::remove_new_orders()
{
        uint32_t i;
        uint64_t order_id;
        
        for (i = 0; i < 10; ++i) {
                if (_deliver_[i] == true) {
                        order_id = create_new_order_key(_warehouse_id, 
                                                        _district_id, 
                                                        _delivery_order_id[i]);
                        delete_record(order_id, NEW_ORDER_TABLE);
                }
        }
}

void delivery::read_orders()
{
        uint32_t i, j;
        uint64_t oorder_key, order_line_key;
        oorder_record *oorders[10];
        order_line_record *order_line;

        for (i = 0; i < 10; ++i) {
                if (_to_deliver[i] == false) {
                        oorders[i] = NULL;
                        continue;
                }
                oorder_key = create_new_order_key(_warehouse_id, i, 
                                                  _deliver_order_id[i]);
                oorders[i] = (oorder_record*)get_read_ref(oorder_key, 
                                                          OORDER_TABLE);
                _customers[i] = oorders[i]->o_c_id;
        }
        
        for (i = 0; i < 10; ++i) {
                _amounts[i] = 0;
                for (j = 0; j < oorders[i]->o_ol_cnt; ++i) {
                        order_line_key = create_order_line_key(_warehouse_id, 
                                                               _district_id, 
                                                               oorders[i]->o_id,
                                                               j);
                        order_line = (order_line_record*)get_read_ref(order_line_key,
                                                                      ORDER_LINE_TABLE);
                        _amounts[i] += order_line->ol_amount;
                }
        }
}

void delivery::update_customer()
{
        uint64_t customer_key;
        customer_record *customer;

        for (i = 0; i < 10; ++i) {
                if (_to_deliver[i] == false)
                        continue;
                customer_key = create_customer_key(_warehouse_id, i, 
                                                   _customers[i]);
                customer = (customer_record*)get_write_ref(customer_key, 
                                                           CUSTOMER_TABLE);
                customer->c_balance += _amounts[i];
                customer->c_delivery_cnt += 1;
        }
}

bool delivery::Run()
{
        read_next_delivery();        
        read_orders();
        remove_new_orders();
        update_customers();
        return true;
}
