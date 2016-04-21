#include <split_action.h>
#include <ycsb.h>
#include <city.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <config.h>
#include <simple_split.h>
#include <set>
#include <split_tpcc.h>

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

uint32_t get_tpcc_partition(void *record, uint32_t type)
{
        if (type == WAREHOUSE_TABLE) {
                return 0;
        } else if (type == DISTRICT_TABLE) {
                return 0;
        } else if (type == CUSTOMER_TABLE) {
                return 0;
        } else {
                assert(false);
                return 0;
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
                //                suppliers[i] = w_id;
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
        /*
        new_order_pc = new insert_new_order(district_pc, no_conf.warehouse_id, 
                                            no_conf.district_id);
        oorder_pc = new insert_oorder(district_pc, customer_pc, 
                                      no_conf.warehouse_id, 
                                      no_conf.district_id, 
                                      no_conf.all_local, 
                                      no_conf.num_items);
        
        for (i = 0; i < no_conf.num_items; ++i) {
                cur_wh = no_conf.suppliers[i];
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
                        if (no_conf.suppliers[j] == it->first) {
                                stock_args[cur_stock]._item_id = no_conf.items[j];
                                stock_args[cur_stock]._quantity = no_conf.quants[j];
                                stock_args[cur_stock]._supplier_wh = no_conf.suppliers[j];
                                cur_stock += 1;
                        }
                }
                stock_pc = new update_stocks(no_conf.warehouse_id, 
                                                    no_conf.district_id, 
                                                    stock_args, 
                                                    it->second);
                stock_pieces.push_back(stock_pc);
                free(stock_args);
        }

       
        ol_pc = new insert_order_lines(no_conf.warehouse_id, no_conf.district_id, 
                                       district_pc, 
                                       stock_pieces, 
                                       warehouse_cnt.size());
        */
                                       
        /* Turn it into a graph */
        txn_graph *graph;
        graph = new txn_graph();
        
        /* Add graph nodes */
        graph_node *warehouse_node = new graph_node();
        partition = get_tpcc_partition(warehouse_pc, WAREHOUSE_TABLE);
        warehouse_node->app = warehouse_pc;
        warehouse_node->partition = partition;
        
        graph_node *district_node = new graph_node();
        partition = get_tpcc_partition(district_pc, WAREHOUSE_TABLE);
        district_node->app = district_pc;
        
        graph_node *customer_node = new graph_node();
        partition = get_tpcc_partition(customer_pc, CUSTOMER_TABLE);
        customer_node->app = customer_pc;
        
        /*
        graph_node *new_order_node = new graph_node();
        new_order_node->app = new_order_pc;
        
        graph_node *oorder_node = new graph_node();
        oorder_node->app = oorder_pc;
        
        graph_node *order_line_node = new graph_node();
        order_line_node->app = ol_pc;
        
        vector<graph_node*> stock_nodes;
        graph_node *temp;
        for (i = 0; i < stock_pieces.size(); ++i) {
                temp = new graph_node();
                temp->app = stock_pieces[i];
                stock_nodes.push_back(temp);
        }
        
        graph->add_edge(district_node, new_order_node);
        graph->add_edge(district_node, oorder_node);
        graph->add_edge(district_node, order_line_node);
        graph->add_edge(warehouse_node, order_line_node);
        graph->add_edge(customer_node, order_line_node);
        */
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
        //        split_ycsb_acc *accumulator_action;
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
        //        accumulator_action = new split_ycsb_acc(read_txns);
        //        (*nodes)[0]->after = accumulator_action;
        
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
        }
        assert(false);
}
