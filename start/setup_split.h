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

#define LCK_TBL_SZ	(((uint64_t)1) << 29)	/* 512 M */
#define SIMPLE_SZ 	2			/* simple action size */

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

        static uint32_t num_split_tables;
        static uint64_t *split_table_sizes;

        /*
         * XXX Fix up this function to take different experiments into account.
         */
        __attribute__((unused)) static void setup_table_info(split_config s_conf)
        {
                assert(num_split_tables == 0 && split_table_sizes == NULL);
                num_split_tables = 1;
                split_table_sizes = 
                        (uint64_t*)zmalloc(num_split_tables*sizeof(uint64_t));
                split_table_sizes[0] = s_conf.num_records;
        }

        static uint32_t get_num_lock_structs()
        {
                return LCK_TBL_SZ / sizeof(lock_struct);        
        }

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
                uint32_t i, sz, count;
                vector<int> *edge_map;

                edge_map = node->in_links;
                if (edge_map == NULL)
                        return 0;
                sz = edge_map->size();
                for (i = 0, count = 0; i < sz; ++i) 
                        if ((*edge_map)[i] == 1)
                                ++count;
                return count;
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

        static split_action* txn_to_piece(txn *txn, uint32_t partition_id)
        {
                uint32_t num_reads, num_rmws, num_writes, max, i;
                big_key *key_array;
                split_action *action;

                action = new split_action(txn, partition_id);
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
                        action->readset.push_back(key_array[i]);
                txn->get_rmws(key_array);
                for (i = 0; i < num_rmws; ++i) 
                        action->writeset.push_back(key_array[i]);
                txn->get_writes(key_array);
                for (i = 0; i < num_writes; ++i) 
                        action->writeset.push_back(key_array[i]);        
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

                piece = txn_to_piece((txn*)node->app, node->partition);
                if (node->in_links == NULL) {
                        piece->set_rvp(NULL);
                        node->txn = piece;
                } else {
                        if ((node_phase = find_phase(node->in_links, phases)) != NULL) {
                                rvp = node_phase->rvp;
                        } else {
                                rvp = new rendezvous_point();
                                node_phase = (txn_phase*)zmalloc(sizeof(txn_phase));
                                node_phase->parent_nodes = node->in_links;
                                node_phase->rvp = rvp;
                                phases->push_back(node_phase);
                                rvp->counter = get_parent_count(node);
                                rvp->to_run = NULL;
                        }
                        piece->set_rvp(rvp);
                        node->txn = piece;                
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

                piece = (split_action*)node->txn;
                find_desc_rvps(node->index, phases, &desc_rvps);
                count = desc_rvps.size();
                rvps = (rendezvous_point**)zmalloc(sizeof(rendezvous_point*)*count);
                
                for (i = 0; i < count; ++i) 
                        rvps[i] = desc_rvps[i]->rvp;
                piece->set_rvp_wakeups(rvps, count);
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
                        actions->push_back((split_action*)topo_list->txn);
                        topo_list = topo_list->topo_link;
                        i += 1;
                }
                assert(i == num_nodes);                        
        }

        static void graph_to_txn(txn_graph *graph, vector<split_action*> *actions)
        {
                assert(actions != NULL && actions->size() == 0);

                uint32_t i, num_nodes;
                vector<txn_phase*> *phases;
                vector<graph_node*> *nodes;
        
                nodes = graph->get_nodes();
                num_nodes = nodes->size();
        
                /* Setup rvps */
                phases = new vector<txn_phase*>();
                for (i = 0; i < num_nodes; ++i) 
                        proc_downstream_node((*nodes)[i], phases);
                for (i = 0; i < num_nodes; ++i)
                        proc_upstream_node((*nodes)[i], phases);
                delete(phases);
        
                /* Output actions, in order */
                gen_piece_array(graph, actions);
        }

        static void setup_single_action(split_config s_conf, workload_config w_conf, 
                                        vector<split_action*> **actions)
        {
                txn_graph *graph;
                vector<split_action*> *generated;
                uint32_t i, sz, partition;
                split_action *cur;

                graph = generate_split_action(w_conf, s_conf.num_partitions);
                generated = new vector<split_action*>();
                graph_to_txn(graph, generated);
                sz = generated->size();
                for (i = 0; i < sz; ++i) {
                        cur = (*generated)[i];
                        partition = cur->get_partition_id();
                        actions[partition]->push_back(cur);
                }
                delete(graph);
                delete(generated);        
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

                temp = (vector<split_action*>**)zmalloc(batch_sz*sizeof(vector<split_action*>*));
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        temp[i] = new vector<split_action*>();
        
                for (i = 0; i < batch_sz; ++i) 
                        setup_single_action(s_conf, w_conf, temp);

                input = vector_to_batch(s_conf.num_partitions, temp);
                free(temp);
                return input;
        }

        static split_action_batch** setup_input(split_config s_conf, workload_config w_conf)
        {
                split_action_batch **batches;

                /* We're generating only two batches for now */
                batches = (split_action_batch**)zmalloc(sizeof(split_action_batch*)*2);
                batches[0] = setup_action_batch(s_conf, w_conf, 10000);
                batches[1] = setup_action_batch(s_conf, w_conf, s_conf.num_txns);
                return batches;
        }

        /*
         * Setup communication queues between executor threads.
         */
        static splt_comm_queue*** setup_comm_queues(split_config s_conf)
        {
                splt_comm_queue ***ret;
                uint32_t i;
        
                ret = (splt_comm_queue***)zmalloc(sizeof(splt_comm_queue**)*
                                                  s_conf.num_partitions);
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        ret[i] = setup_queues<split_action*>(s_conf.num_partitions, 
                                                             1024);
                return ret;
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
        static struct lock_table_config setup_lock_table_config(uint32_t cpu, 
                                                                __attribute__((unused)) split_config s_conf)
        {

                uint32_t num_lock_structs;
                num_lock_structs = get_num_lock_structs();
                struct lock_table_config ret = {
                        cpu,
                        num_split_tables,
                        split_table_sizes,
                        num_lock_structs,
                };
                return ret;
        }

        static struct split_executor_config setup_exec_config(uint32_t cpu, 
                                                              uint32_t num_partitions, 
                                                              uint32_t partition_id,
                                                              splt_comm_queue **ready_queues,
                                                              splt_inpt_queue *input_queue,
                                                              split_config s_conf)
        {
                struct split_executor_config exec_conf;
                struct lock_table_config lck_conf;

                lck_conf = setup_lock_table_config(cpu, s_conf);
                exec_conf = {
                        cpu,
                        num_partitions,
                        partition_id,
                        ready_queues,
                        input_queue,
                        lck_conf,
                };
                return exec_conf;
        }

        /*
         * Setup executor threads.
         */
        static split_executor** setup_threads(split_config s_conf, 
                                              splt_inpt_queue **in_queues,
                                              __attribute__((unused)) splt_inpt_queue **out_queues)
        {
                split_executor **ret;
                splt_comm_queue ***comm_queues;
                split_executor_config conf;
                uint32_t i;

                ret = (split_executor**)
                        zmalloc(sizeof(split_executor*)*s_conf.num_partitions);
                comm_queues = setup_comm_queues(s_conf);
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        conf = setup_exec_config(i, s_conf.num_partitions, i, 
                                                 comm_queues[i],
                                                 in_queues[i],
                                                 s_conf);
                        ret[i] = new(i) split_executor(conf);
                }
                return ret;
        }

        /* 
         * XXX Measurements need to be more sophisticated. For now, use a single batch 
         * to warmup, and another one to measure throughput.
         */ 
        static void do_experiment(split_action_batch** inputs, 
                                  splt_inpt_queue **input_queues, 
                                  splt_inpt_queue **output_queues,
                                  uint32_t num_batches,
                                  split_config s_conf)
        {
                uint32_t i;
                split_action_batch *cur_batch;

                /* One warmup, one real */
                assert(num_batches == 2);
        
                /* Do warmup */
                cur_batch = inputs[0];
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        input_queues[i]->EnqueueBlocking(cur_batch[i]);
                for (i = 0; i < s_conf.num_partitions; ++i)
                        output_queues[i]->DequeueBlocking();
        
                /* Do real batch */
                cur_batch = inputs[1];
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        input_queues[i]->EnqueueBlocking(cur_batch[i]);
                for (i = 0; i < s_conf.num_partitions; ++i)
                        output_queues[i]->DequeueBlocking();
        }

        static void split_experiment(split_config s_conf, workload_config w_conf)
        {
                num_split_tables = 0;
                split_table_sizes = NULL;

                splt_inpt_queue **input_queues, **output_queues;
                split_action_batch **inputs;
                uint32_t num_batches;
        
                num_batches = 2;
                input_queues = setup_input_queues(s_conf);
                output_queues = setup_input_queues(s_conf);

                inputs = setup_input(s_conf, w_conf);
                setup_threads(s_conf, input_queues, output_queues);
                do_experiment(inputs, input_queues, output_queues, num_batches, s_conf);
        }
};

#endif 		// SETUP_SPLIT_H_
