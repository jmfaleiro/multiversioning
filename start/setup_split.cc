#include <config.h>
#include <split_action.h>
#include <split_executor.h>
#include <sys/time.h>
#include <common.h>
#include <fstream>
#include <db.h>
#include <graph.h>

#define LCK_TBL_SZ	(((uint64_t)1) << 29)	/* 512 M */
#define SIMPLE_SZ 	2			/* simple action size */

uint32_t num_split_tables = 0;
uint64_t *split_table_sizes = NULL;

struct txn_phase {
        vector<int> *parent_nodes;
        rendezvous_point *rvp;
};

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
}

static uint32_t get_num_lock_structs()
{
        return LCK_TBL_SZ / sizeof(lock_struct);        
}

static uint32_t get_partition(const big_key *key, uint32_t num_partitions)
{
        uint64_t temp;
        temp = key->table_id;
        temp <<= 32;
        temp |= num_partitions;
        return Hash128to64(std::make_pair(key->key, temp)) % num_partitions;
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
        uint32_t i, j, num_phases, phase_sz;
        txn_phase *cur_phase;

        num_phases = phases->size();
        for (i = 0; i < num_phases; ++i) 
                if (vector_eq(in_edges, (*phases)[i]->parent_nodes))
                        return (*phases)[i];
        return NULL;
}

static split_action* txn_to_piece(txn *txn)
{
        return NULL;
}

static uint32_t get_parent_count(vector<int> *edge_map)
{
        uint32_t i, sz, count;
        
        sz = edge_map->size();
        for (i = 0, count = 0; i < sz; ++i) 
                if ((*edge_map)[i] == 1)
                        ++count;
        return count;
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

/*
 * First phase of processing. Associate a rendezvous point with a downstream 
 * piece.
 */
static void proc_downstream_node(graph_node *node, vector<txn_phase*> *phases)
{
        txn_phase *node_phase;
        split_action *piece;
        rendezvous_point *rvp;

        piece = txn_to_piece((txn*)node->app);
        if ((node_phase = find_phase(node->in_links, phases)) == NULL) {
                rvp = node_phase->rvp;
        } else {
                rvp = new rendezvous_point();
                node_phase = (txn_phase*)zmalloc(sizeof(txn_phase));
                node_phase->parent_nodes = node->in_links;
                node_phase->rvp = rvp;
                phases->push_back(node_phase);
                rvp->counter = get_parent_count(node->in_links);
                
        }
        piece->set_rvp(rvp);
        node->txn = piece;
}

/*
 * Second phase of processing. Associate an upstream piece with its rendezvous 
 * points.
 */
static void proc_upstream_node(graph_node *node, vector<txn_phase*> *phases)
{
        rendezvous_point **rvps;
        uint32_t count, i, j, sz;
        txn_phase *ph;
        split_action *piece;

        piece = (split_action*)node->txn;
        count = get_index_count(node->index, phases);
        rvps = (rendezvous_point**)zmalloc(sizeof(rendezvous_point*)*count);

        sz = phases->size();
        for (i = 0, j = 0; i < sz; ++i) {
                ph = (*phases)[i];
                if ((*(ph->parent_nodes))[node->index] == 1) {
                        rvps[j] = ph->rvp;
                        j += 1;
                }
        }
        piece->set_rvp_wakeups(rvps, count);
}

static void traverse_graph(graph_node *node, txn_graph *graph, int *processed, 
                           split_action **actions, 
                           int *cursor)
{
        /* This fn should not be called on processed nodes */ 
        assert(processed[node->index] == 0);        

        uint32_t i, sz;
        vector<int> *out_edges;
        vector<graph_node*> *nodes;

        nodes = graph->get_nodes();
        
        /* 
         * cursor points into the next free slot in the actions array, and must 
         * therefore be less then the number of nodes 
         */
        assert(nodes->size() < *cursor);
        actions[*cursor] = (split_action*)node->txn;
        *cursor += 1;
        processed[node->index] = 1;
        
        for (i = 0; i < sz; ++i) 
                if ((*out_edges)[i] == 1 && processed[i] == 0)
                        traverse_graph((*nodes)[i], graph, processed, actions, 
                                       cursor);
}

static split_action** gen_piece_array(txn_graph *graph)
{
        split_action **ret;
        uint32_t num_nodes, i;
        vector<int> *root_bitmap;
        vector<graph_node*> *nodes;
        int *processed, cursor;

        nodes = graph->get_nodes();
        num_nodes = nodes->size();
        processed = (int*)zmalloc(sizeof(int)*num_nodes);
        ret = (split_action**)zmalloc(sizeof(split_action*)*num_nodes);
        root_bitmap = graph->get_roots();
        for (i = 0; i < num_nodes; ++i) 
                if ((*root_bitmap)[i] == 1)
                        traverse_graph((*nodes)[i], graph, processed, ret, &cursor);
        return ret;
}

static split_action** graph_to_txn(txn_graph *graph)
{
        uint32_t i, num_nodes;
        vector<txn_phase*> *phases;
        split_action **ret;
        vector<graph_node*> *nodes;
        
        nodes = graph->get_nodes();
        num_nodes = nodes->size();
        phases = new vector<txn_phase*>();
        for (i = 0; i < num_nodes; ++i) 
                proc_downstream_node((*nodes)[i], phases);
        for (i = 0; i < num_nodes; ++i)
                proc_upstream_node((*nodes)[i], phases);
        free(phases);
        ret = gen_piece_array(graph);
        return ret;
}



/* 
 * Take a transaction specified as a graph as input, and generate a split-action
 * as output 
 */
static vector<split_action*> gen_split_action(txn_graph *graph)
{
        uint32_t i, j, num_nodes;
        vector<int> *root_bitmap, *edges;
        vector<int> visited;
        vector<split_action*> ret;
        vector<graph_node*> *graph_nodes;
        graph_node *node;

        root_bitmap = graph->get_roots();
        num_nodes = root_bitmap->size();
        graph_nodes = graph->get_nodes();
        for (i = 0; i < num_nodes; ++i) {
                visited[i] = 0;
        }

        for (i = 0; i < num_nodes; ++i) {
                if ((*root_bitmap)[i] == 0)
                        continue;
                
                assert(visited[i] == 0); /* Can't have visited a root */
                node = (*graph_nodes)[i];
                edges = node->out_links;
                for (j = 0; j < edges->size(); ++j) {
                        
                }
        }
        
        /* All nodes must be processed */
        for (i = 0; i < num_nodes; ++i) 
                assert(visited[i] == 1);

        return ret;
}

static split_action* gen_piece(big_key *keys, uint32_t num_keys, 
                               __attribute__((unused)) uint32_t num_partitions)
{
        assert(num_keys > 0);
        
        uint32_t i;
        split_action *action;
        
        action = new split_action(NULL);
        for (i = 0; i < num_keys; ++i) {
                
                /* All keys must belong to the same partition */
                assert(get_partition(&keys[0], num_partitions) == 
                       get_partition(&keys[i], num_partitions));                
                action->writeset.push_back(keys[i]);
        }
        return action;
}

/*
 * Generate a single transaction which performs two RMW operations.
 */
static split_action* gen_single_action(split_config s_config, 
                                       RecordGenerator *gen)
{
        split_action *root, *leaf;
        uint32_t i, first_partition, second_partition;
        big_key keys[2];

        keys[0].table_id = 0;
        keys[1].table_id = 0;
        keys[0].key = gen->GenNext();
        keys[1].key = gen->GenNext();

        first_partition = get_partition(&keys[0], s_config.num_partitions);
        second_partition = get_partition(&keys[1], s_config.num_partitions);
        if (first_partition == second_partition) {
                root = gen_piece(&keys[0], 2, s_config.num_partitions);
        } else {
                root = gen_piece(&keys[0], 1, s_config.num_partitions);
                leaf = gen_piece(&keys[1], 1, s_config.num_partitions);
        }        
        return root;
}

/*
 * Generate a batch of simple transactions consisting of two RMW transactions.
 */
static struct split_action_batch gen_batch(split_config s_config)
{
        uint32_t i;
        split_action_batch ret;
        split_action **actions;
        size_t alloc_sz;

        alloc_sz = sizeof(split_action*)*s_config.num_txns;
        actions = (split_action**)zmalloc(alloc_sz);
        ret.num_actions = s_config.num_txns;
        for (i = 0; i < s_config.num_txns; ++i) {
                actions[i] = gen_single_action(s_config, NULL);
        }
        ret.actions = actions;
}

/*
 * Setup transactions consisting of two pieces. Each piece touches a single 
 * record. 
 */
static void setup_simple_experiment(split_config s_config)
{
        uint32_t i;
        for (i = 0; i < s_config.num_txns; ++i) {

        }
}

static void setup_input(split_config s_conf, workload_config w_conf)
{
        
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
                                                        split_config s_conf)
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
                                      splt_inpt_queue **in_queues)
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

static void do_experiment()
{
}

void split_experiment(split_config s_conf, workload_config w_confx)
{
        splt_inpt_queue **input_queues;

        input_queues = setup_input_queues(s_conf);
        //        setup_input();
        setup_threads(s_conf, input_queues);
        //        do_experiment();
}
