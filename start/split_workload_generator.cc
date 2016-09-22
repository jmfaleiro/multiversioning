#include <split_action.h>
#include <ycsb.h>
#include <city.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <config.h>
#include <simple_split.h>
#include <set>
#include <split_tpcc.h>

uint32_t rand_salt;
extern RecordGenerator *my_gen;

extern uint64_t gen_unique_key(RecordGenerator *gen, 
                               std::set<uint64_t> *seen_keys);

struct split_new_order_conf {
        uint32_t warehouse_id;
        uint32_t district_id;
        uint32_t customer_id;
        uint32_t num_items;
        uint32_t *items;
        uint32_t *suppliers;
        uint32_t *quants;
        bool all_local;
};

struct split_payment_conf {
        uint32_t w_id;
        uint32_t d_id;
        uint32_t c_id;
};

uint64_t simple_record0, simple_record1;
bool init = false;

uint32_t get_partition(uint64_t record, __attribute__((unused)) uint32_t table, 
                       uint32_t num_partitions)
{
        return record % num_partitions;
//         uint64_t temp;
//         temp = table;
//         temp = (temp << 32);
//         temp = (temp | num_partitions);        
//         return Hash128to64(std::make_pair(record, temp)) % num_partitions;
}

uint32_t get_tpcc_warehouse_partition(uint32_t warehouse, uint32_t type, uint32_t num_partitions)
{

        uint32_t temp;
        //        assert(type == WAREHOUSE_TABLE);
        if (type == WAREHOUSE_TABLE) {
                return warehouse % num_partitions;
        } else if (type == STOCK_TABLE) {
                return (num_partitions - 1 - (warehouse % num_partitions));
                //                temp = warehouse % num_partitions;
                //                return num_partitions - 1 - temp;
        }

        //        uint64_t temp = ((uint64_t)type << 32) | num_partitions;
        //        return Hash128to64(std::make_pair(warehouse, type)) % num_partitions;
}

uint32_t get_tpcc_history_partition(uint32_t num_partitions)
{
        return num_partitions - 1;
}

uint32_t get_tpcc_district_partition(uint32_t warehouse, uint32_t district, 
                                     uint32_t type,
                                     uint32_t num_partitions)
{
        assert(type == DISTRICT_TABLE || type == NEW_ORDER_TABLE || 
               type == CUSTOMER_TABLE ||
               type == OORDER_TABLE ||
               type == ORDER_LINE_TABLE ||
               type == HISTORY_TABLE);
        //        return  % num_partitions;
        /*

                if (type == DISTRICT_TABLE)
                        return 0;
                else if (type == CUSTOMER_TABLE)
                        return 1;
                uint32_t index;
                index = warehouse*NUM_DISTRICTS + district;
                return 2 + index % (num_partitions - 2);
        */

        if (type == DISTRICT_TABLE)
                return (warehouse * (NUM_DISTRICTS + 6) + district) % num_partitions;
        else if (type == CUSTOMER_TABLE)
                return (warehouse * (NUM_DISTRICTS + 6) + district + 1) % num_partitions;
        else if (type == HISTORY_TABLE)
                return (warehouse * (NUM_DISTRICTS + 6) + district + 2) % num_partitions;
        else if (type == NEW_ORDER_TABLE) {
                return (warehouse * (NUM_DISTRICTS + 6) + district + 3) % num_partitions;
        } else if (type == OORDER_TABLE) {
                return (warehouse * (NUM_DISTRICTS + 6) + district + 4) % num_partitions;
        } else if (type == ORDER_LINE_TABLE) {
                return (warehouse * (NUM_DISTRICTS + 6) + district + 5) % num_partitions;
        } else
                assert(false);


        /*
        if (type == DISTRICT_TABLE)
                return 1;
        else if (type == CUSTOMER_TABLE)
                return 2;
        return 3 + ((warehouse*NUM_DISTRICTS + district) % (num_partitions - 3));
        */
        //        assert(district < NUM_DISTRICTS);
        //        uint32_t index;
        //        index = warehouse*NUM_DISTRICTS + district;
                //        return warehouse % num_partitions;

        uint64_t temp0, temp1;
        temp1 = ((uint64_t)type << 32) | num_partitions;
        temp0 = ((((uint64_t)warehouse) << 32) | district);
        return Hash128to64(std::make_pair(temp0, temp1)) % num_partitions;
}

uint32_t get_tpcc_stock_partition(uint32_t warehouse, uint32_t item, uint32_t type, uint32_t num_partitions)
{
        assert(type == STOCK_TABLE);
        return item % num_partitions;
}

uint32_t get_tpcc_partition(uint32_t warehouse, uint32_t district, uint32_t type, 
                            uint32_t num_partitions)
{
        switch (type) {
        case WAREHOUSE_TABLE:
                return get_tpcc_warehouse_partition(warehouse, type, num_partitions);
        case STOCK_TABLE:
                //                return get_tpcc_stock_partition(warehouse, district, type, num_partitions);
                return get_tpcc_warehouse_partition(warehouse, type, num_partitions);
        case DISTRICT_TABLE:
        case CUSTOMER_TABLE:
        case NEW_ORDER_TABLE:
        case ORDER_LINE_TABLE:
        case OORDER_TABLE:
        case HISTORY_TABLE:
                return get_tpcc_district_partition(warehouse, district, type, num_partitions);                
        default:
                assert(false);
        }
}

graph_node* find_node(uint32_t partition, txn_graph *graph)
{
        vector<graph_node*> *nodes;
        uint32_t i, num_nodes;

        nodes = graph->get_nodes();
        num_nodes = nodes->size();
        for (i = 0; i < num_nodes; ++i) {
                if ((*nodes)[i]->partition == partition)
                        return (*nodes)[i];
        }
        return NULL;
}

split_payment_conf gen_payment_conf(workload_config conf)
{
        assert(conf.experiment == TPCC_SUBSET);

        split_payment_conf ret;

        ret.w_id = (uint32_t)rand() % conf.num_warehouses;
        ret.d_id = (uint32_t)rand() % NUM_DISTRICTS;
        ret.c_id = (uint32_t)rand() % NUM_CUSTOMERS;
        
        return ret;
}

split_new_order_conf gen_neworder_conf(workload_config conf)
{
        assert(conf.experiment == TPCC_SUBSET);
        
        uint32_t w_id, d_id, c_id, *quants, nitems, i, temp, *items, *suppliers;
        UniformGenerator item_gen(NUM_ITEMS);
        set<uint64_t> seen_items;
        split_new_order_conf ret;

        
        //        assert(thread < conf.num_warehouses);
        w_id = (uint64_t)rand() % conf.num_warehouses;
        assert(w_id < conf.num_warehouses);
        
        d_id = (uint32_t)rand() % NUM_DISTRICTS;
        assert(d_id < NUM_DISTRICTS);
        
        c_id = (uint32_t)rand() % NUM_CUSTOMERS;
        assert(c_id < NUM_CUSTOMERS);
        
        nitems = 5 + ((uint32_t)rand() % 11);
        items = (uint32_t*)zmalloc(sizeof(uint32_t)*nitems);
        quants = (uint32_t*)zmalloc(sizeof(uint32_t)*nitems);
        suppliers = (uint32_t*)zmalloc(sizeof(uint32_t)*nitems);
        
        for (i = 0; i < nitems; ++i) {
                items[i] = gen_unique_key(&item_gen, &seen_items);
                quants[i] = 1 + ((uint32_t)rand() % 10);
                //suppliers[i] = w_id;
                temp = rand() % 100;
                if (temp == 0) {                        

                        do {
                                suppliers[i] = rand() % conf.num_warehouses;
                        } while (suppliers[i] == w_id && conf.num_warehouses > 1);
                } else {

                        suppliers[i] = w_id;
                }
        }
        
        ret.warehouse_id = w_id;
        ret.district_id = d_id;
        ret.customer_id = c_id;
        ret.num_items = nitems;
        ret.items = items;
        ret.suppliers = suppliers;
        ret.quants = quants;
        return ret;
}

txn_graph* gen_payment(workload_config conf, __attribute__((unused)) uint32_t num_partitions)
{
        using namespace split_payment;
        
        uint32_t time, partition;
        split_payment_conf p_conf;
        float h_amount;
        update_warehouse *wh_piece;
        update_district *d_piece;
        update_customer *c_piece;
        insert_history *h_piece;
        txn_graph *graph;
        graph_node *wh_node, *d_node, *c_node, *h_node;

        p_conf = gen_payment_conf(conf);
        time = 0;
        h_amount = 1.0*((uint32_t)rand() % 5000);

        wh_piece = new update_warehouse(p_conf.w_id, h_amount);
        d_piece = new update_district(p_conf.w_id, p_conf.d_id, h_amount);
        c_piece = new update_customer(p_conf.w_id, p_conf.d_id, p_conf.c_id, 0,
                                      false, 
                                      h_amount);
        h_piece = new insert_history(wh_piece, d_piece, p_conf.w_id, 
                                     p_conf.d_id, 
                                     p_conf.c_id,
                                     p_conf.w_id, 
                                     p_conf.d_id, 
                                     0,
                                     false,
                                     h_amount, 
                                     time);

        graph = new txn_graph();

        wh_node = new graph_node();
        partition = get_tpcc_partition(p_conf.w_id, p_conf.d_id, WAREHOUSE_TABLE, num_partitions);
        wh_node->app = wh_piece;
        wh_node->partition = partition;
        graph->add_node(wh_node);

        d_node = new graph_node();
        partition = get_tpcc_partition(p_conf.w_id, p_conf.d_id, DISTRICT_TABLE, num_partitions);
        d_node->app = d_piece;
        d_node->partition = partition;
        graph->add_node(d_node);
        
        c_node = new graph_node();
        partition = get_tpcc_partition(p_conf.w_id, p_conf.d_id, CUSTOMER_TABLE, num_partitions);
        c_node->app = c_piece;
        c_node->partition = partition;
        graph->add_node(c_node);
        
        h_node = new graph_node();
        partition = get_tpcc_partition(p_conf.w_id, p_conf.d_id, HISTORY_TABLE, num_partitions);
        h_node->app = h_piece;
        h_node->partition = partition;
        graph->add_node(h_node);
        
        graph->add_edge(wh_node, h_node);
        graph->add_edge(d_node, h_node);
        graph->add_edge(c_node, h_node);
        
        return graph;
}

txn_graph* gen_new_order(workload_config conf, __attribute__((unused)) uint32_t num_partitions)
{
        using namespace split_new_order;
        assert(conf.experiment == TPCC_SUBSET);
        
        uint32_t cur_wh, i, cur_stock, j, partition;
        stock_update_data *stock_args;
        std::unordered_map<uint32_t, uint32_t> warehouse_cnt;
        std::set<uint32_t> warehouses;
        split_new_order_conf no_conf;
        read_warehouse *warehouse_pc;
        update_district *district_pc;
        read_customer *customer_pc;
        insert_new_order *new_order_pc;
        update_stocks *stock_pc;
        insert_oorder *oorder_pc;
        vector<update_stocks*> stock_pieces;
        insert_order_lines *ol_pc;
        
        no_conf = gen_neworder_conf(conf);
        warehouse_pc = new read_warehouse(no_conf.warehouse_id);
        district_pc = new update_district(no_conf.warehouse_id, no_conf.district_id);
        customer_pc = new read_customer(no_conf.warehouse_id, 
                                        no_conf.district_id, 
                                        false, 
                                        0, 
                                        no_conf.customer_id);

        
        oorder_pc = new insert_oorder(district_pc, customer_pc, 
                                      no_conf.warehouse_id, 
                                      no_conf.district_id, 
                                      no_conf.all_local, 
                                      no_conf.num_items);

        for (i = 0; i < no_conf.num_items; ++i) {
                cur_wh = get_tpcc_partition(no_conf.suppliers[i], no_conf.items[i], STOCK_TABLE, num_partitions);
                if (warehouse_cnt.count(cur_wh) == 0) 
                        warehouse_cnt[cur_wh] = 1;
                else 
                        warehouse_cnt[cur_wh] += 1;
        }
        
        for (auto it = warehouse_cnt.begin();
             it != warehouse_cnt.end();
             ++it) {
                stock_args = (stock_update_data*)zmalloc(sizeof(stock_update_data)*it->second);
                cur_stock = 0;
                for (j = 0; j < no_conf.num_items; ++j) {
                        cur_wh = get_tpcc_partition(no_conf.suppliers[j], no_conf.items[j], STOCK_TABLE, num_partitions);
                        if (cur_wh == it->first) {
                                stock_args[cur_stock]._item_id = no_conf.items[j];
                                stock_args[cur_stock]._quantity = no_conf.quants[j];
                                stock_args[cur_stock]._supplier_wh = no_conf.suppliers[j];
                                cur_stock += 1;
                        }
                }
                stock_pc = new update_stocks(no_conf.warehouse_id, 
                                             no_conf.district_id, 
                                             it->first,
                                             stock_args, 
                                             it->second);
                stock_pieces.push_back(stock_pc);
                free(stock_args);
        }
        new_order_pc = new insert_new_order(district_pc, customer_pc,
                                            no_conf.warehouse_id, 
                                            no_conf.district_id,
                                            no_conf.all_local,
                                            no_conf.num_items,
                                            &stock_pieces);


        ol_pc = new insert_order_lines(no_conf.warehouse_id, 
                                       no_conf.district_id, 
                                       district_pc, 
                                       &stock_pieces);

        /* Turn it into a graph */
        txn_graph *graph;
        graph = new txn_graph();
        
        /* Add graph nodes */
        graph_node *warehouse_node = new graph_node();
        partition = get_tpcc_partition(no_conf.warehouse_id, no_conf.district_id,
                                       WAREHOUSE_TABLE, num_partitions);
        warehouse_node->app = warehouse_pc;
        warehouse_node->partition = partition;
        graph->add_node(warehouse_node);

        vector<graph_node*> stock_nodes;
        graph_node *temp;
        for (i = 0; i < stock_pieces.size(); ++i) {
                temp = new graph_node();
                temp->app = stock_pieces[i];
                temp->partition = stock_pieces[i]->_partition;
                //                assert(temp->partition == warehouse_node->partition);
                stock_nodes.push_back(temp);
                graph->add_node(temp);
        }

        graph_node *district_node = new graph_node();
        partition = get_tpcc_partition(no_conf.warehouse_id, 
                                       no_conf.district_id, 
                                       DISTRICT_TABLE,
                                       num_partitions);
        district_node->app = district_pc;
        district_node->partition = partition;
        graph->add_node(district_node);
        
        graph_node *customer_node = new graph_node();
        partition = get_tpcc_partition(no_conf.warehouse_id, no_conf.district_id,
                                       CUSTOMER_TABLE, num_partitions);
        customer_node->app = customer_pc;
        customer_node->partition = partition;
        graph->add_node(customer_node);        
        
        

        /*
        if (no_conf.warehouse_id % 4 == 0) {
                new_order_node->partition = 2;
                graph->add_node(new_order_node);
                graph->add_edge(district_node, new_order_node);
        } else {
                new_order_node->partition = 4;
        }
        */
        

        graph_node *oorder_node = new graph_node();
        oorder_node->app = oorder_pc;
        oorder_node->partition = get_tpcc_partition(no_conf.warehouse_id, 
                                                    no_conf.district_id, 
                                                    OORDER_TABLE, 
                                                    num_partitions);
        graph->add_node(oorder_node);
        graph->add_edge(district_node, oorder_node);
        graph->add_edge(customer_node, oorder_node);
        graph->add_edge(warehouse_node, oorder_node);

        graph_node *new_order_node = new graph_node();
        new_order_node->app = new_order_pc;
        new_order_node->partition = get_tpcc_partition(no_conf.warehouse_id, no_conf.district_id, NEW_ORDER_TABLE, num_partitions);
        
        //        std::cout << new_order_node->partition << "\n";
        //        assert(new_order_node->partition >= 2);
        graph->add_node(new_order_node);
        graph->add_edge(district_node, new_order_node);
        graph->add_edge(customer_node, new_order_node);
        graph->add_edge(warehouse_node, new_order_node);


        graph_node *order_line_node = new graph_node();
        order_line_node->app = ol_pc;
        order_line_node->partition = get_tpcc_partition(no_conf.warehouse_id, 
                                                        no_conf.district_id, 
                                                        ORDER_LINE_TABLE,
                                                        num_partitions);

        graph->add_node(order_line_node);
        graph->add_edge(district_node, order_line_node);
        graph->add_edge(warehouse_node, order_line_node);
        graph->add_edge(customer_node, order_line_node);


        assert(stock_pieces.size() == stock_nodes.size());
        for (i = 0; i < stock_pieces.size(); ++i) {
                graph->add_edge(stock_nodes[i], new_order_node);
                graph->add_edge(stock_nodes[i], order_line_node);
                graph->add_edge(stock_nodes[i], oorder_node);
        }


        //        graph->add_edge(new_order_node, order_line_node);
        //        assert(graph->get_nodes()->size() == + stock_nodes.size());
        return graph;
}

txn_graph* gen_read_write(RecordGenerator *gen, workload_config conf, 
                          uint32_t num_partitions)
{
        uint32_t i, j, key, partition, write_check;
        uint32_t *read_array;
        vector<uint64_t> writeset, readset, args;
        assert(conf.txn_size % 2 == 0 && conf.experiment == YCSB_RW);
        std::set<uint64_t> seen;
        txn_graph *read_graph, *write_graph;
        graph_node *cur_node, *writer;
        vector<graph_node*> *nodes, read_nodes, write_nodes;
        vector<split_ycsb_read*> read_txns;
        split_ycsb_acc *accumulator_action;
        split_ycsb_read *current_read;

        write_graph = new txn_graph();
        read_graph = new txn_graph();

        /* Gen reads and writes */
        for (i = 0; i < conf.txn_size; ++i) {
                key = gen_unique_key(gen, &seen);
                if (i % 2 == 0) {
                        writeset.push_back(key);
                        partition = get_partition(key, 0, num_partitions);
                        if ((cur_node = find_node(partition, write_graph)) == NULL) {
                                cur_node = new graph_node();
                                cur_node->partition = partition;
                                write_graph->add_node(cur_node);
                        }
                } else {
                        readset.push_back(key);
                        partition = get_partition(key, 0, num_partitions);
                        if ((cur_node = find_node(partition, read_graph)) == NULL) {
                                cur_node = new graph_node();
                                cur_node->partition = partition;
                                read_graph->add_node(cur_node);
                                read_nodes.push_back(cur_node);
                        }                                
                }                                
        }

        /* Create read buffers */
        read_array = (uint32_t*)zmalloc(sizeof(uint32_t)*read_nodes.size());
        
        /* Create read actions */        
        nodes = read_graph->get_nodes();
        write_check = 0;
        for (i = 0; i < nodes->size(); ++i) {
                args.clear();
                cur_node = (*nodes)[i];
                partition = cur_node->partition;
                assert(partition != INT_MAX);
                for (j = 0; j < conf.txn_size/2; ++j) {
                        if (get_partition(readset[j], 0, num_partitions) == 
                            partition) {
                                args.push_back(readset[j]);
                                write_check += 1;
                        }
                }
                assert(args.size() != 0);
                current_read = new split_ycsb_read(&read_array[i], args);
                read_txns.push_back(current_read);
                cur_node->app = current_read;
        }        
        assert(write_check == conf.txn_size / 2);
        
        /* Create accumulator */
        //        accumulator_action = NULL;
        accumulator_action = new split_ycsb_acc(read_txns);
        (*nodes)[0]->after = accumulator_action;
        
        /* Create write actions */
        nodes = write_graph->get_nodes();
        write_check = 0;
        for (i = 0; i < nodes->size(); ++i) {
                args.clear();
                cur_node = (*nodes)[i];
                partition = cur_node->partition;
                assert(partition != INT_MAX);
                for (j = 0; j < conf.txn_size/2; ++j) {
                        if (get_partition(writeset[j], 0, num_partitions) == 
                            partition) {
                                args.push_back(writeset[j]);
                                write_check += 1;
                        }
                }
                assert(args.size() != 0);
                cur_node->app = new split_ycsb_update(read_array,
                                                      read_nodes.size(),
                                                      args);
                write_nodes.push_back(cur_node);
        }        
        assert(write_check == conf.txn_size / 2);
        
        for (i = 0; i < nodes->size(); ++i) {
                writer = new graph_node();
                writer->partition = (*nodes)[i]->partition;
                writer->app = (*nodes)[i]->app;
                read_graph->add_node(writer);
                
                for (j = 0; j < read_nodes.size(); ++j) {
                        cur_node = read_nodes[j];
                        read_graph->add_edge(cur_node, writer);
                }
        }
        delete(write_graph);
        return read_graph;
}


txn_graph* gen_ycsb_update(RecordGenerator *gen, workload_config conf, 
                           uint32_t num_partitions,
                           std::set<uint64_t> *seen_keys)
{
        //        assert(conf.txn_size == 9);
        assert(conf.experiment == YCSB_UPDATE);
        set<uint32_t> partitions;
        vector<uint64_t> writes, args;
        uint64_t updates[10], key;
        uint32_t i, j, write_check, partition;
        vector<graph_node*> abortables;

        vector<graph_node*> *nodes;
        graph_node *cur_node;
        txn_graph *graph;
        
        graph = new txn_graph();

        /* Generate updates */
        for (i = 0; i < 10; ++i) 
                updates[i] = rand();
        
        for (i = 0; i < conf.txn_size; ++i) {
                key = gen_unique_key(gen, seen_keys);
                writes.push_back(key);
                partition = get_partition(key, 0, num_partitions);
                if ((cur_node = find_node(partition, graph)) == NULL) {
                        cur_node = new graph_node();
                        cur_node->partition = partition;
                        graph->add_node(cur_node);
                }                                
        }
        //        assert(seen->size() == writes.size());
        
        /* Create actions */
        nodes = graph->get_nodes();
        write_check = 0;
        for (i = 0; i < nodes->size(); ++i) {
                args.clear();
                cur_node = (*nodes)[i];
                partition = cur_node->partition;
                assert(partition != INT_MAX);
                for (j = 0; j < conf.txn_size; ++j) {
                        if (get_partition(writes[j], 0, num_partitions) == 
                            partition) {
                                args.push_back(writes[j]);
                                write_check += 1;
                        }
                }
                assert(args.size() != 0);
                cur_node->app = new ycsb_update(args, updates);
                /* XXX REMOVE THIS */
                // cur_node->abortable = true;
        }        
        assert(write_check == conf.txn_size);
        return graph;
}

txn_graph* gen_ycsb_readonly(RecordGenerator *gen, workload_config conf, 
                             uint32_t num_partitions)
{
        set<uint32_t> partitions;
        vector<uint64_t> writes, args;
        uint64_t key;
        uint32_t i, j, write_check, partition;
        vector<graph_node*> abortables;
        std::set<uint64_t> seen_keys;

        vector<graph_node*> *nodes;
        graph_node *cur_node;
        txn_graph *graph;
        
        graph = new txn_graph();

        for (i = 0; i < conf.read_txn_size; ++i) {
                //                key = i;

                key = gen_unique_key(gen, &seen_keys);
                writes.push_back(key);
                partition = get_partition(key, 0, num_partitions);
                if ((cur_node = find_node(partition, graph)) == NULL) {
                        cur_node = new graph_node();
                        cur_node->partition = partition;
                        graph->add_node(cur_node);
                } 
                
        }
        //        assert(seen->size() == writes.size());
        
        /* Create actions */
        nodes = graph->get_nodes();
        write_check = 0;
        for (i = 0; i < nodes->size(); ++i) {
                args.clear();
                cur_node = (*nodes)[i];
                partition = cur_node->partition;
                assert(partition != INT_MAX);
                for (j = 0; j < conf.read_txn_size; ++j) {
                        if (get_partition(writes[j], 0, num_partitions) == 
                            partition) {
                                args.push_back(writes[j]);
                                write_check += 1;
                        }
                }
                assert(args.size() != 0);
                cur_node->app = new ycsb_readonly(args);
                /* XXX REMOVE THIS */
                // cur_node->abortable = true;
        }        
        assert(write_check == conf.read_txn_size);
        return graph;
}

txn_graph* gen_ycsb_abortable(RecordGenerator *gen, workload_config conf, 
                              uint32_t num_partitions)
{
        txn_graph *abort_graph, *commit_graph;
        workload_config conf_copy;
        vector<graph_node*> *abort_nodes, *commit_nodes, new_nodes, old_nodes;
        graph_node *cur, *new_node;
        uint32_t i, j, sz, abort_sz;
        std::set<uint64_t> seen_keys;

        conf_copy = conf;
        conf_copy.txn_size = conf.abort_pos;        
        abort_graph = gen_ycsb_update(gen, conf_copy, num_partitions, 
                                      &seen_keys);
        abort_nodes = abort_graph->get_nodes();
        sz = abort_nodes->size();
        for (i = 0; i < sz; ++i) {
                (*abort_nodes)[i]->abortable = true;
        }

        conf_copy.txn_size = conf.txn_size - conf.abort_pos;
        commit_graph = gen_ycsb_update(gen, conf_copy, num_partitions, 
                                       &seen_keys);
        commit_nodes = commit_graph->get_nodes();
        sz = commit_nodes->size();
        for (i = 0; i < sz; ++i) {
                cur = (*commit_nodes)[i];
                new_node = new graph_node();
                new_node->app = cur->app;
                new_node->abortable = false;
                new_node->partition = cur->partition;
                new_nodes.push_back(new_node);
        }
        delete(commit_graph);
        
        abort_sz = abort_nodes->size();
        for (i = 0; i < abort_sz; ++i) {
                old_nodes.push_back((*abort_nodes)[i]);
        }
        
        for (i = 0; i < sz; ++i) {
                abort_graph->add_node(new_nodes[i]);
                for (j = 0; j < abort_sz; ++j) {
                        abort_graph->add_edge(old_nodes[j], new_nodes[i]);
                }
        }
        return abort_graph;
}



/*
 * Generate two abortable ppieces, and one non-abortable piece that follows.
 */
txn_graph* generate_abortable_action(RecordGenerator *gen, workload_config conf,
                                     uint32_t num_partitions)

{
        assert(conf.experiment == 2);

        txn_graph *graph;
        graph_node *node0, *node1, *node2;
        uint64_t rec0, rec1, rec2;
        uint32_t partition0, partition1, partition2;
        simple_split *txn;
        vector<uint64_t> records;
        std::set<uint64_t> seen_keys;

        graph = new txn_graph();

        rec0 = gen_unique_key(gen, &seen_keys);
        partition0 = get_partition(rec0, 0, num_partitions);
        rec2 = gen_unique_key(gen, &seen_keys);
        partition2 = get_partition(rec2, 0, num_partitions);
        while (true) {
                rec1 = gen_unique_key(gen, &seen_keys);
                partition1 = get_partition(rec1, 0, num_partitions);
                if (partition0 != partition1)
                        break;                
        }
        
        /* First piece */
        records.clear();
        records.push_back(rec0);
        txn = new simple_split(records);
        node0 = new graph_node();
        node0->app = txn;
        node0->partition = partition0;
        node0->abortable = true;
        graph->add_node(node0);
        
        /* Second piece */
        records.clear();
        records.push_back(rec1);
        txn = new simple_split(records);
        node1 = new graph_node();
        node1->app = txn;
        node1->partition = partition1;
        node1->abortable = true;
        graph->add_node(node1);

        /* Third piece */
        records.clear();
        records.push_back(rec2);
        txn = new simple_split(records);
        node2 = new graph_node();
        node2->app = txn;
        node2->partition = partition2;
        graph->add_node(node2);
        
        /* Add edges */
        graph->add_edge(node0, node2);
        graph->add_edge(node1, node2);
        return graph;
}


/*
 * Generate txns with three pieces. Two pieces signal a single downstream piece.
 */
txn_graph* generate_dual_rvp(RecordGenerator *gen, workload_config conf, 
                             uint32_t num_partitions)
{
        assert(conf.experiment == 1);

        txn_graph *graph;
        graph_node *node0, *node1, *node2;
        uint64_t rec0, rec1, rec2;
        uint32_t partition0, partition1, partition2;
        simple_split *txn;
        vector<uint64_t> records;
        std::set<uint64_t> seen_keys;
        
        graph = new txn_graph();

        rec0 = gen_unique_key(gen, &seen_keys);
        partition0 = get_partition(rec0, 0, num_partitions);
        rec2 = gen_unique_key(gen, &seen_keys);
        partition2 = get_partition(rec2, 0, num_partitions);
        while (true) {
                rec1 = gen_unique_key(gen, &seen_keys);
                partition1 = get_partition(rec1, 0, num_partitions);
                if (partition0 != partition1)
                        break;                
        }
        
        /* First piece */
        records.clear();
        records.push_back(rec0);
        txn = new simple_split(records);
        node0 = new graph_node();
        node0->app = txn;
        node0->partition = partition0;
        graph->add_node(node0);
        
        /* Second piece */
        records.clear();
        records.push_back(rec1);
        txn = new simple_split(records);
        node1 = new graph_node();
        node1->app = txn;
        node1->partition = partition1;
        graph->add_node(node1);

        /* Third piece */
        records.clear();
        records.push_back(rec2);
        txn = new simple_split(records);
        node2 = new graph_node();
        node2->app = txn;
        node2->partition = partition2;
        graph->add_node(node2);
        
        /* Add edges */
        graph->add_edge(node0, node2);
        graph->add_edge(node1, node2);
        return graph;
}

txn_graph* generate_simple_action(RecordGenerator *gen, workload_config conf, 
                                  uint32_t num_partitions)
{
        txn_graph *graph;
        graph_node *node0, *node1;
        static uint64_t rec0, rec1;
        uint32_t partition0, partition1;
        simple_split *txn;
        vector<uint64_t> records;
        std::set<uint64_t> seen_keys;

        /* XXX Generate simple actions. No other experiment supported */
        if (conf.experiment != 0) 
                assert(false);
        
        graph = new txn_graph();
        

        /* First record goes to an even partition */
        simple_record0 = gen_unique_key(gen, &seen_keys);
        partition0 = get_partition(simple_record0, 0, num_partitions);
        simple_record1 = gen_unique_key(gen, &seen_keys);
        partition1 = get_partition(simple_record1, 0, num_partitions);
        rec0 = simple_record0;
        rec1 = simple_record1;
        if (partition0 == partition1) {

                /* Both records belong to a single partition */
                records.push_back(rec0);
                records.push_back(rec1);
                txn = new simple_split(records);
                node0 = new graph_node();
                node0->app = txn;
                node0->partition = partition0;
                graph->add_node(node0);
        } else {

                /* Dual-partition transaction */
                records.push_back(rec0);
                txn = new simple_split(records);
                node0 = new graph_node();
                node0->app = txn;
                node0->partition = partition0;
                graph->add_node(node0);
                
                records.clear();
                records.push_back(rec1);
                txn = new simple_split(records);
                node1 = new graph_node();
                node1->app = txn;
                node1->partition = partition1;
                graph->add_node(node1);
                graph->add_edge(node0, node1);
        }
        
        return graph;
}

txn_graph* generate_split_action(workload_config conf, uint32_t num_partitions)
{
        if (conf.distribution == 0 && my_gen == NULL) 
                my_gen = new UniformGenerator(conf.num_records);
        else if (conf.distribution == 1 && my_gen == NULL)
                my_gen = new ZipfGenerator(conf.num_records, conf.theta);
                
        if (conf.experiment == 0)
                return generate_simple_action(my_gen, conf, num_partitions);
        else if (conf.experiment == 1)
                return generate_dual_rvp(my_gen, conf, num_partitions);
        else if (conf.experiment == 2)
                return generate_abortable_action(my_gen, conf, num_partitions);
        else if (conf.experiment == YCSB_UPDATE) {
                if (conf.read_pct > 0 && (uint32_t)rand() % 100 < conf.read_pct)
                        return gen_ycsb_readonly(my_gen, conf, num_partitions);
                else
                        return gen_ycsb_abortable(my_gen, conf, num_partitions);
        } else if (conf.experiment == YCSB_RW) {
                return gen_read_write(my_gen, conf, num_partitions);
        } else if (conf.experiment == TPCC_SUBSET) {
                if (rand() % 2 == 0)
                        return gen_new_order(conf, num_partitions);
                else 
                        return gen_payment(conf, num_partitions);
        }
        assert(false);
}
