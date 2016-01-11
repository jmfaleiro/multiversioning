#include <split_action.h>
#include <city.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <config.h>
#include <simple_split.h>

extern RecordGenerator *my_gen;

uint32_t get_partition(uint64_t record, uint32_t table, uint32_t num_partitions)
{
        uint64_t temp;
        temp = table;
        temp = (temp << 32);
        temp = (temp | num_partitions);        
        return Hash128to64(std::make_pair(record, temp)) % num_partitions;
}

txn_graph* generate_simple_action(RecordGenerator *gen, workload_config conf, 
                                  uint32_t num_partitions)
{
        txn_graph *graph;
        graph_node *node0, *node1;
        uint64_t rec0, rec1;
        uint32_t partition0, partition1;
        simple_split *txn;
        vector<uint64_t> records;

        /* XXX Generate simple actions. No other experiment supported */
        if (conf.experiment != 0) 
                assert(false);
        
        graph = new txn_graph();
        rec0 = gen->GenNext();
        rec1 = gen->GenNext();
        partition0 = get_partition(rec0, 0, num_partitions);
        partition1 = get_partition(rec1, 0, num_partitions);
        
        if (partition0 == partition1) {
                
                /* Both records belong to a single partition */
                records.push_back(rec0);
                records.push_back(rec1);
                txn = new simple_split(records);
                node0 = new graph_node();
                node0->app = txn;
                graph->add_node(node0);
        } else {
                
                /* Dual-partition transaction */
                records.push_back(rec0);
                txn = new simple_split(records);
                node0 = new graph_node();
                node0->app = txn;
                graph->add_node(node0);
                
                records.clear();
                records.push_back(rec1);
                txn = new simple_split(records);
                node1 = new graph_node();
                node1->app = txn;
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
        assert(false);
}
