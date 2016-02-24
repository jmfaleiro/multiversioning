#ifndef 	SETUP_SPLIT_H_
#define 	SETUP_SPLIT_H_

#include <config.h>
#include <split_action.h>
#include <split_executor.h>
#include <split_workload_generator.h>
#include <sys/time.h>
#include <common.h>
#include <fstream>
#include <db.h>
#include <graph.h>
#include <algorithm>

#define LCK_TBL_SZ	(((uint64_t)1) << 29)	/* 512 M */
#define SIMPLE_SZ 	2			/* simple action size */

extern uint32_t GLOBAL_RECORD_SIZE;
extern uint32_t get_partition(uint64_t record, uint32_t table, 
                              uint32_t num_partitions);

struct txn_phase {
        vector<int> *parent_nodes;
        rendezvous_point *rvp;
};

enum graph_node_state {
        STATE_UNVISITED = 0,
        STATE_PROCESSING,
        STATE_VISITED,
};

class setup_split {
public:
        
        static vector<uint32_t> *partitions_txns;
        static uint32_t num_split_tables;
        static uint64_t *split_table_sizes;
        static uint64_t *lock_table_sizes;

        static uint32_t get_num_batches(split_config s_conf)
        {
                uint32_t ret;

                ret = s_conf.num_txns / s_conf.epoch_size;
                if (s_conf.num_txns % s_conf.epoch_size > 0)
                        ret += 1;
                return ret;
        }

        /*
         * XXX Fix up this function to take different experiments into account.
         */
        static void setup_table_info(split_config s_conf)
        {
                assert(num_split_tables == 0 && split_table_sizes == NULL);
                num_split_tables = 1;
                split_table_sizes = 
                        (uint64_t*)zmalloc(num_split_tables*sizeof(uint64_t));
                split_table_sizes[0] = s_conf.num_records;
                lock_table_sizes = (uint64_t*)zmalloc(num_split_tables*sizeof(uint64_t));
                lock_table_sizes[0] = s_conf.num_records;
        }

        static void setup_single_table(int partition, split_config s_conf, 
                                       Table ***tbl)
        {
                Table **init_tbl;
                TableConfig t_conf;
                //                assert(s_conf.experiment == YCSB_UPDATE);
                if (s_conf.experiment == YCSB_UPDATE || 
                    s_conf.experiment == 0 || 
                    s_conf.experiment == 1 ||
                    s_conf.experiment == 2) {
                        t_conf.tableId = 0;
                        t_conf.numBuckets = (split_table_sizes[0])/s_conf.num_partitions;
                        t_conf.startCpu = partition;
                        t_conf.endCpu = partition+1;
                        t_conf.freeListSz = (split_table_sizes[0]*2)/s_conf.num_partitions;
                        // t_conf.valueSz = SPLIT_RECORD_SIZE(GLOBAL_RECORD_SIZE);
                        t_conf.valueSz = sizeof(split_record);
                        t_conf.recordSize = 0;
                        init_tbl = (Table**)zmalloc(sizeof(Table*));
                        init_tbl[0] = new (partition) Table(t_conf);
                } else {
                        assert(false);
                }
                
                tbl[partition] = init_tbl;
        }

        static Table*** setup_tables(split_config s_conf)
        {
                uint32_t i;
                Table ***ret;

                //                assert(s_conf.experiment == YCSB_UPDATE);

                ret = (Table***)zmalloc(sizeof(Table**)*s_conf.num_partitions);
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        setup_single_table(i, s_conf, ret);                
                return ret;
        }

        /* XXX Initializes only a single table */
        static void init_tables(split_config s_conf, Table ***tbls)
        {
                uint32_t partition, index;
                uint32_t partition_sizes[s_conf.num_partitions];
                uint32_t p_indices[s_conf.num_partitions];
                char *arrays[s_conf.num_partitions], *buf;
                uint64_t i, j;
                split_record record;

                //                assert(s_conf.experiment == YCSB_UPDATE);
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        partition_sizes[i] = 0;
                
                
                for (i = 0; i < split_table_sizes[0]; ++i) {
                        partition = get_partition(i, 0, s_conf.num_partitions);
                        partition_sizes[partition] += 1;
                }

                for (i = 0; i < s_conf.num_partitions; ++i) {
                        arrays[i] = (char*)alloc_mem(partition_sizes[i]*GLOBAL_RECORD_SIZE, i);
                        memset(arrays[i], 0x0, partition_sizes[i]*GLOBAL_RECORD_SIZE);
                        p_indices[i] = 0;
                }
                
                for (i = 0; i < split_table_sizes[0]; ++i) {
                        record.key.key = (uint64_t)i;
                        record.key.table_id = 0;
                        record.epoch = 0;
                        record.key_struct = NULL;
                        partition = get_partition(i, 0, s_conf.num_partitions);
                        index = p_indices[partition];
                        p_indices[partition] += 1;
                        buf = (char*)&arrays[partition][GLOBAL_RECORD_SIZE*index];
                        record.value = buf;
                        for (j = 0; j < 1000/sizeof(uint64_t); ++j)
                                ((uint64_t*)buf)[j] = rand();
                        tbls[partition][0]->Put(i, &record);
                }
                return;
        }

        /*
        static uint32_t get_num_lock_structs()
        {
                return LCK_TBL_SZ / sizeof(lock_struct);        
        }
        */

        static bool edge_list_eq(vector<int> *first, vector<int> *second)
        {
                uint32_t i, j, sz;
                int cur;
                
                if (first->size() != second->size())
                        return false;
                
                sz = first->size();
                for (i = 0; i < sz; ++i) {
                        cur = (*first)[i];
                        for (j = 0; j < sz; ++j) {
                                if (cur == (*second)[j])
                                        break;
                        }                        
                        if (j == sz)
                                return false;
                }
                return true;
                
        }

        static bool vector_eq(vector<int> *first, vector<int> *second)
        {
                assert(first->size() == second->size());

                uint32_t i, sz;
                sz = first->size();
                for (i = 0; i < sz; ++i) 
                        if ((*first)[i] != (*second)[i])
                                return false;
                return true;
        }

        static txn_phase* find_phase(vector<int> *in_edges, vector<txn_phase*> *phases)
        {
                uint32_t i, num_phases;
                num_phases = phases->size();
                for (i = 0; i < num_phases; ++i) {
                        if (edge_list_eq(in_edges, (*phases)[i]->parent_nodes))
                                return (*phases)[i];
                }
                return NULL;
        }

        static uint32_t get_parent_count(graph_node *node)
        {
                vector<int> *edge_map;

                edge_map = node->in_links;
                if (edge_map == NULL)
                        return 0;
                return edge_map->size();
        }

        static void find_desc_rvps(int index, vector<txn_phase*> *phases, 
                                   vector<txn_phase*> *desc)
        {
                assert(desc->size() == 0);
                
                uint32_t i, j, num_phases, phase_sz;
                vector<int> *cur_phase;

                num_phases = phases->size();
                for (i = 0; i < num_phases; ++i) {
                        cur_phase = (*phases)[i]->parent_nodes;
                        phase_sz = cur_phase->size();
                        for (j = 0; j < phase_sz; ++j) {
                                if (index == (*cur_phase)[j]) {
                                        desc->push_back((*phases)[i]);
                                        break;
                                }
                        }
                }
        }

        static uint32_t get_index_count(int index, vector<txn_phase*> *phases)
        {
                uint32_t i, sz, count;
        
                sz = phases->size();
                for (i = 0, count = 0; i < sz; ++i)
                        if ((*(*phases)[i]->parent_nodes)[index] == 1)
                                count += 1;
                return count;
        }

        static split_action* txn_to_piece(txn *txn, uint32_t partition_id, 
                                          bool dependency_flag, bool can_commit)
        {
                uint32_t num_reads, num_rmws, num_writes, max, i;
                big_key *key_array;
                split_action *action;

                action = new split_action(txn, partition_id, dependency_flag, 
                                          can_commit);
                txn->set_translator(action);
                num_reads = txn->num_reads();
                num_writes = txn->num_writes();
                num_rmws = txn->num_rmws();

                if (num_reads >= num_writes && num_reads >= num_rmws) 
                        max = num_reads;
                else if (num_rmws >= num_writes && num_rmws >= num_reads)
                        max = num_rmws;
                else 
                        max = num_writes;
                key_array = (big_key*)zmalloc(sizeof(big_key)*max);
                
                txn->get_reads(key_array);
                for (i = 0; i < num_reads; ++i) 
                        action->_readset.push_back(key_array[i]);
                
                txn->get_rmws(key_array);
                for (i = 0; i < num_rmws; ++i) {
                        action->_writeset.push_back(key_array[i]);
                }

                txn->get_writes(key_array);
                for (i = 0; i < num_writes; ++i) {
                        action->_writeset.push_back(key_array[i]);
                }
                return action;
        }

        /*
         * First phase of processing. Associate a rendezvous point with a downstream 
         * piece.
         */
        static void proc_downstream_node(graph_node *node, vector<txn_phase*> *phases)
        {
                txn_phase *node_phase;
                split_action *piece;
                rendezvous_point *rvp;
                
                /* Check that the partition on the node has been initialized */
                assert(node->partition != INT_MAX);

                if (node->in_links == NULL) {
                        piece = txn_to_piece(node->app, node->partition, false, 
                                             node->abortable);
                        piece->set_rvp(NULL);
                        node->t = piece;
                } else {
                        piece = txn_to_piece(node->app, node->partition, true, 
                                             node->abortable);
                        if ((node_phase = find_phase(node->in_links, phases)) != NULL) {
                                rvp = node_phase->rvp;
                        } else {
                                rvp = new rendezvous_point();
                                node_phase = (txn_phase*)zmalloc(sizeof(txn_phase));
                                node_phase->parent_nodes = node->in_links;
                                node_phase->rvp = rvp;
                                phases->push_back(node_phase);
                                barrier();
                                rvp->counter = get_parent_count(node);
                                rvp->num_actions = rvp->counter;
                                barrier();
                                rvp->to_run = NULL;
                        }
                        piece->set_rvp(rvp);
                        node->t = piece;                
                }
        }

        /*
         * Second phase of processing. Associate an upstream piece with its rendezvous 
         * points.
         */
        static void proc_upstream_node(graph_node *node, vector<txn_phase*> *phases)
        {
                rendezvous_point **rvps;
                uint32_t count, i;
                split_action *piece;
                vector<txn_phase*> desc_rvps;

                piece = node->t;
                find_desc_rvps(node->index, phases, &desc_rvps);
                count = desc_rvps.size();
                rvps = (rendezvous_point**)zmalloc(sizeof(rendezvous_point*)*count);
                
                for (i = 0; i < count; ++i) 
                        rvps[i] = desc_rvps[i]->rvp;
                piece->set_rvp_wakeups(rvps, count);
        }
        
        static void find_abortables(txn_graph *graph) 
        {
                vector<graph_node*> *nodes;
                graph_node *cur_node;
                commit_rvp *rvp;
                split_action **actions;
                uint32_t abortable_count, i, j, sz;
                
                nodes = graph->get_nodes();
                sz = nodes->size();
                abortable_count = 0;
                for (i = 0; i < sz; ++i) {
                        cur_node = (*nodes)[i];
                        if (cur_node->abortable == true) 
                                abortable_count += 1;                        
                }                
                
                actions = (split_action**)zmalloc(sizeof(split_action*)*abortable_count);
                rvp = (commit_rvp*)zmalloc(sizeof(commit_rvp));
                rvp->num_actions = abortable_count;
                rvp->to_notify = actions;
                barrier();
                rvp->num_committed = 0;
                rvp->status = (uint64_t)ACTION_UNDECIDED;
                barrier();
                
                for (i = 0, j = 0; i < sz; ++i) {
                        cur_node = (*nodes)[i];
                        if (cur_node->abortable == true) {
                                actions[j] = cur_node->t;
                                cur_node->t->set_commit_rvp(rvp);
                                j += 1;
                        }
                }
        }

        static void traverse_graph(graph_node *node, txn_graph *graph, 
                                   int *processed, 
                                   graph_node **topo_list)
        {
                /* "DAG" has a cycle! */
                assert(processed[node->index] != 1);
                
                /* Node's been processed */
                if (processed[node->index] == 2)
                        return;

                uint32_t i, sz, index;
                vector<int> *out_edges;
                vector<graph_node*> *nodes;

                processed[node->index] = 1;
                nodes = graph->get_nodes();
                out_edges = node->out_links;
                if (out_edges != NULL) {
                        sz = out_edges->size();
                        for (i = 0; i < sz; ++i) {
                                index = (*out_edges)[i];
                                assert(index < nodes->size());
                                traverse_graph((*nodes)[index], graph, 
                                               processed, 
                                               topo_list);
                        }
                }
                processed[node->index] = 2;
                assert(node->topo_link == NULL);
                node->topo_link = *topo_list;
                *topo_list = node;
        }

        static void gen_piece_array(txn_graph *graph, vector<split_action*> *actions)
        {
                /* 
                 * Actions contains the set of generated split_actions. 
                 * At this point, should be empty. 
                 */
                assert(actions->size() == 0);

                uint32_t num_nodes, i;
                vector<graph_node*> *nodes;
                int *processed;
                graph_node *topo_list;
                

                topo_list = NULL;
                nodes = graph->get_nodes();
                num_nodes = nodes->size();
                processed = (int*)zmalloc(sizeof(int)*num_nodes);
                for (i = 0; i < num_nodes; ++i) 
                        traverse_graph((*nodes)[i], graph, processed, &topo_list);
                free(processed);
                i = 0;
                while (topo_list != NULL) {
                        actions->push_back(topo_list->t);
                        topo_list = topo_list->topo_link;
                        i += 1;
                }
                assert(i == num_nodes);                        
        }

        static void graph_to_txn(txn_graph *graph, vector<split_action*> *actions)
        {
                assert(actions != NULL && actions->size() == 0);

                uint32_t i, num_nodes;
                vector<txn_phase*> phases;
                vector<graph_node*> *nodes;
        
                nodes = graph->get_nodes();
                num_nodes = nodes->size();
        
                /* Setup rvps */
                for (i = 0; i < num_nodes; ++i) 
                        proc_downstream_node((*nodes)[i], &phases);
                for (i = 0; i < num_nodes; ++i)
                        proc_upstream_node((*nodes)[i], &phases);
                find_abortables(graph);

                /* Get rid of phases */
                for (i = 0; i < phases.size(); ++i) 
                        free(phases[i]);
        
                /* Output actions, in order */
                gen_piece_array(graph, actions);
        }

        static void setup_single_action(split_config s_conf, workload_config w_conf, 
                                        vector<split_action*> **actions)
        {
                txn_graph *graph;
                vector<split_action*> generated;
                uint32_t i, sz, partition;
                split_action *cur;

                graph = generate_split_action(w_conf, s_conf.num_partitions);
                graph_to_txn(graph, &generated);
                sz = generated.size();
                for (i = 0; i < sz; ++i) {
                        cur = generated[i];
                        partition = cur->get_partition_id();
                        actions[partition]->push_back(cur);
                }
                delete(graph);
        }


        static split_action_batch* vector_to_batch(uint32_t num_partitions, 
                                                   vector<split_action*> **inputs)
        {
                uint32_t i, j, num_txns;
                split_action_batch *ret;        
                split_action **actions;

                ret = (split_action_batch*)zmalloc(sizeof(split_action_batch)*num_partitions);
                for (i = 0; i < num_partitions; ++i) {
                        num_txns = inputs[i]->size();
                        actions = (split_action**)zmalloc(sizeof(split_action*)*num_txns);
                        for (j = 0; j < num_txns; ++j) {
                                actions[j] = (*inputs[i])[j];
                                assert(actions[j]->get_partition_id() == i);
                        }
                        ret[i].actions = actions;
                        ret[i].num_actions = num_txns;
                }
                return ret;
        }

        /* Setup a bunch of actions. # of action batches == num_partitions */
        static split_action_batch* setup_action_batch(split_config s_conf, 
                                                      workload_config w_conf,
                                                      uint32_t batch_sz)
        {
                uint32_t i;
                vector<split_action*> **temp;
                split_action_batch *input;

                temp = (vector<split_action*>**)zmalloc(s_conf.num_partitions*sizeof(vector<split_action*>*));
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        temp[i] = new vector<split_action*>();
        
                for (i = 0; i < batch_sz; ++i) 
                        setup_single_action(s_conf, w_conf, temp);

                input = vector_to_batch(s_conf.num_partitions, temp);
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        delete(temp[i]);
                free(temp);
                return input;
        }

        static split_action_batch** setup_input(split_config s_conf, workload_config w_conf)
        {
                split_action_batch **batches;
                uint32_t num_batches, i, remaining;
                
                num_batches = get_num_batches(s_conf);

                /* We're generating only two batches for now */
                batches = (split_action_batch**)zmalloc((1+num_batches)*sizeof(split_action_batch*));
                batches[0] = setup_action_batch(s_conf, w_conf, 1);

                std::cerr << num_batches << "\n";                
                remaining = s_conf.num_txns;
                for (i = 1; i < num_batches + 1; ++i) {
                        if (remaining >= s_conf.epoch_size) {
                                batches[i] = setup_action_batch(s_conf, w_conf, s_conf.epoch_size);
                                remaining -= s_conf.epoch_size;
                        } else if (s_conf.epoch_size - remaining > 0) {
                                batches[i] = setup_action_batch(s_conf, w_conf, remaining);
                                remaining -= remaining;
                        } else {
                                assert(false);
                        }
                }
                assert(remaining == 0);
                return batches;
        }


        /*
         * Setup action input queues for executors.
         */
        static splt_inpt_queue** setup_input_queues(split_config s_conf)
        {
                splt_inpt_queue **ret;
                ret = setup_queues<split_action_batch>(s_conf.num_partitions, 1024);
                return ret;
        }

        /*
         * Setup lock table config.
         */
        /*
        static struct lock_table_config setup_lock_table_config(uint32_t cpu, 
                                                                __attribute__((unused)) split_config s_conf)
        {

                uint32_t num_lock_structs;
                num_lock_structs = get_num_lock_structs();
                struct lock_table_config ret = {
                        cpu,
                        num_split_tables,
                        lock_table_sizes,
                        num_lock_structs,
                };
                return ret;
        }
        */

        static struct split_executor_config setup_exec_config(uint32_t cpu, 
                                                              uint32_t num_partitions, 
                                                              uint32_t partition_id,
                                                              splt_inpt_queue *input_queue,
                                                              splt_inpt_queue *output_queue,
                                                              split_config s_conf, 
                                                              Table **tables)
        {
                struct split_executor_config exec_conf;

                exec_conf = {
                        cpu,
                        num_partitions,
                        partition_id,
                        s_conf.num_outstanding,
                        input_queue,
                        output_queue,
                        tables,
                };
                return exec_conf;
        }

        /*
         * Setup executor threads.
         */
        static split_executor** setup_threads(split_config s_conf, 
                                              splt_inpt_queue **in_queues,
                                              splt_inpt_queue **out_queues,
                                              Table ***tbls)
                                              
        {
                split_executor **ret;
                split_executor_config conf;
                uint32_t i;

                ret = (split_executor**)
                        zmalloc(sizeof(split_executor*)*s_conf.num_partitions);
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        assert(in_queues[i] != NULL && out_queues[i] != NULL);
                        conf = setup_exec_config(i, s_conf.num_partitions, i, 
                                                 in_queues[i],
                                                 out_queues[i],
                                                 s_conf,
                                                 tbls[i]);
                        ret[i] = new(i) split_executor(conf);
                        ret[i]->Run();
                }
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        ret[i]->WaitInit();
                return ret;
        }

        /* 
         * XXX Measurements need to be more sophisticated. For now, use a single batch 
         * to warmup, and another one to measure throughput.
         */ 
        static timespec do_experiment(split_action_batch** inputs, 
                                      splt_inpt_queue **input_queues, 
                                      splt_inpt_queue **output_queues,
                                      split_config s_conf)
        {
                uint32_t i, j, num_batches;
                split_action_batch *cur_batch;
                timespec start_time, end_time;

                /* Do warmup */
                cur_batch = inputs[0];
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        input_queues[i]->EnqueueBlocking(cur_batch[i]);
                }
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        output_queues[i]->DequeueBlocking();
                }
                
                /* Do real batch */
                num_batches = get_num_batches(s_conf);
                barrier();
                clock_gettime(CLOCK_REALTIME, &start_time);
                barrier();
                for (j = 0; j < num_batches; ++j) {
                        for (i = 0; i < s_conf.num_partitions; ++i) 
                                input_queues[i]->EnqueueBlocking(inputs[j+1][i]);
                }
                
                for (j = 0; j < num_batches; ++j) {
                        for (i = 0; i < s_conf.num_partitions; ++i) 
                                output_queues[i]->DequeueBlocking();
                }
                
                barrier();
                clock_gettime(CLOCK_REALTIME, &end_time);
                barrier();
                
                return diff_time(end_time, start_time);
        }

        static void write_output(split_config conf,
                                 double elapsed_milli, 
                                 workload_config w_conf)
        {
                std::ofstream result_file;

                result_file.open("split.txt", std::ios::app | std::ios::out);
                result_file << "split ";
                result_file << "time:" << elapsed_milli << " ";
                result_file << "txns:" << conf.num_txns << " ";
                result_file << "threads:" << conf.num_partitions << " ";
                result_file << "records:" << conf.num_records << " ";
                result_file << "read_pct:" << conf.read_pct << " ";
                if (conf.experiment == 0) 
                        result_file << "test " << " ";
                else if (conf.experiment == 1) 
                        result_file << "rvp test" << " ";
                else if (conf.experiment == 2) 
                        result_file << "abort test " << " ";
                else if (conf.experiment == 3) 
                        result_file << "small_bank" << " ";
                else if (conf.experiment == 4) 
                        result_file << "ycsb_update" << " " << "abort_pos:" << w_conf.abort_pos << " ";
                else
                        assert(false);
 
                if (conf.distribution == 0) 
                        result_file << "uniform ";
                else if (conf.distribution == 1) 
                        result_file << "zipf theta:" << conf.theta << " ";
                else
                        assert(false);

                result_file << "\n";
                result_file.close();  
                std::cout << "Time elapsed: " << elapsed_milli << " ";
                std::cout << "Num txns: " << conf.num_txns << "\n";
        }

        static void write_sizes() 
        {
                assert(partitions_txns != NULL);
                std::ofstream result_file;
                uint32_t i;
                double diff, cur;
                
                std::sort(partitions_txns->begin(), partitions_txns->end());
                result_file.open("txn_sizes.txt", std::ios::app | std::ios::out);
                
                diff = 1.0 / (double)(partitions_txns->size());
                cur = 0.0;
                for (i = 0; i < partitions_txns->size(); ++i) {
                        result_file << (*partitions_txns)[i] << " " << cur << "\n";
                        cur += diff;
                }
                
                result_file.close();
        }
        
        static void split_experiment(split_config s_conf, workload_config w_conf)
        {
                num_split_tables = 0;
                split_table_sizes = NULL;

                splt_inpt_queue **input_queues, **output_queues;
                split_action_batch **inputs;
                timespec exp_time;
                double elapsed_milli;
                Table ***tables;

                setup_table_info(s_conf);
                tables = setup_tables(s_conf);
                init_tables(s_conf, tables);

                input_queues = setup_input_queues(s_conf);
                output_queues = setup_input_queues(s_conf);
                
                std::cerr << "Setup queues\n";

                inputs = setup_input(s_conf, w_conf);
                
                std::cerr << "Setup input\n";
                setup_threads(s_conf, input_queues, output_queues, tables);
                std::cerr << "Setup database threads\n";
                exp_time = do_experiment(inputs, input_queues, output_queues, s_conf);
                std::cerr << "Done experiment\n";
                elapsed_milli =
                        1000.0*exp_time.tv_sec + exp_time.tv_nsec/1000000.0;
                write_output(s_conf, elapsed_milli, w_conf);
        }
};

#endif 		// SETUP_SPLIT_H_
