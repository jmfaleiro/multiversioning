#include <split_action.h>
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

txn_graph* generate_simple_action(RecordGenerator *gen, workload_config conf, 
                                  uint32_t num_partitions)
{
        txn_graph *graph;
        graph_node *node0, *node1;
        static uint64_t rec0, rec1;
        uint32_t partition0, partition1;
        simple_split *txn;
        vector<uint64_t> records;
        //        std::set<uint64_t> seen_keys;

        /* XXX Generate simple actions. No other experiment supported */
        if (conf.experiment != 0) 
                assert(false);
        
        graph = new txn_graph();
        
        if (init == false) {
                /* First record goes to an even partition */
                while (true) {
                        simple_record0 = gen->GenNext();
                        partition0 = get_partition(simple_record0, 0, num_partitions);
                        if (partition0 % 2 == 0)
                                break;
                }

                /* Second record goes to an odd partition */
                while (true) {
                        simple_record1 = gen->GenNext();
                        partition1 = get_partition(simple_record1, 0, num_partitions);
                        if (partition1 % 2 == 1)
                                break;
                }
                init = true;                
        } else {
                partition0 = get_partition(simple_record0, 0, num_partitions);
                assert(partition0 % 2 == 0);
                
                partition1 = get_partition(simple_record1, 0, num_partitions);
                assert(partition1 % 2 == 1);
        }
        rec0 = simple_record0;
        rec1 = simple_record1;
        if (partition0 == partition1) {
                assert(false);

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
        assert(false);
}
