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

uint32_t get_partition(uint64_t record, uint32_t table, uint32_t num_partitions)
{
        uint64_t temp;
        temp = table;
        temp = (temp << 32);
        temp = (temp | num_partitions);        
        return Hash128to64(std::make_pair(record, temp)) % num_partitions;
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
                           uint32_t num_partitions)
{
        assert(conf.experiment == YCSB_UPDATE);
        set<uint64_t> seen;
        set<uint32_t> partitions;
        vector<uint64_t> writes, args;
        uint64_t updates[10], key;
        uint32_t i, j, write_check, partition;
        
        vector<graph_node*> *nodes;
        graph_node *cur_node;
        txn_graph *graph;
        
        graph = new txn_graph();

        /* Generate updates */
        for (i = 0; i < 10; ++i) 
                updates[i] = rand();
        
        /* Generate keys */
        for (i = 0; i < conf.txn_size; ++i) {
                key = gen_unique_key(gen, &seen);
                writes.push_back(key);
                partition = get_partition(key, 0, num_partitions);
                if ((cur_node = find_node(partition, graph)) == NULL) {
                        cur_node = new graph_node();
                        cur_node->partition = partition;
                        graph->add_node(cur_node);
                }                                
        }

        
        /* Create actions */
        nodes = graph->get_nodes();
        write_check = 0;
        num_partitions = nodes->size();
        for (i = 0; i < num_partitions; ++i) {
                cur_node = (*nodes)[i];
                partition = cur_node->partition;
                assert(partition != INT_MAX);
                for (j = 0; j < conf.txn_size; ++j) {
                        if (get_partition(writes[i], 0, num_partitions) == 
                            partition) {
                                args.push_back(writes[i]);
                        }
                }
                cur_node->app = new ycsb_update(args, updates);
        }        
        assert(write_check == conf.txn_size);
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
        if (conf.distribution == 0) 
                my_gen = new UniformGenerator(conf.num_records);
        else if (conf.distribution == 1)
                my_gen = new ZipfGenerator(conf.num_records, conf.theta);
        else
                assert(false);
                
        if (conf.experiment == 0)
                return generate_simple_action(my_gen, conf, num_partitions);
        else if (conf.experiment == YCSB_UPDATE)
                return gen_ycsb_update(my_gen, conf, num_partitions);
        assert(false);
}
