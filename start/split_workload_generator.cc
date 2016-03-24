#include <split_action.h>
#include <ycsb.h>
#include <city.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <config.h>
#include <simple_split.h>
#include <set>

extern RecordGenerator *my_gen;

extern uint64_t gen_unique_key(RecordGenerator *gen, 
                               std::set<uint64_t> *seen_keys);

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
        }
        assert(false);
}
