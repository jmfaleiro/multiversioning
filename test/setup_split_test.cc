#include <gtest/gtest.h>
#include <graph.h>
#include <iostream>
#include <cpuinfo.h>
#include <setup_split.h>
#include <simple_split.h>

struct node_internal {
        void *txn;
        void *app;
};

class graph_test : public ::testing::Test {

private:
        node_internal *nodes;

protected:
        
        static rendezvous_point** get_rvps(split_action *action)
        {
                return action->rvps;
        }

        static uint32_t get_rvp_count(split_action *action)
        {
                return action->rvp_count;
        }
        
        static void* get_app(split_action *action)
        {
                return action->t;
        }

        static bool find_rvp(split_action *action, rendezvous_point *rvp)
        {
                split_action *start;
                start = rvp->to_run;

                while (start != NULL) {
                        if (action == start)
                                return true;
                        start = start->next;
                }
                return false;
        }

        
        virtual void SetUp() {
                
        }
};

class setup_split_test : public ::testing::Test {

protected:
        virtual void SetUp() {

        }

};

template<class T>
static int find_node(T node, vector<T> *node_set)
{
        uint32_t i, sz;
        
        sz = node_set->size();
        for (i = 0; i < sz; ++i) 
                if ((*node_set)[i] == node) 
                        return (int)i;
        return -1;
}


TEST_F(graph_test, empty)
{
        txn_graph *graph;
        vector<graph_node*> *nodes;
        vector<int> *roots;

        graph = new txn_graph();
        nodes = graph->get_nodes();
        roots = graph->get_roots();
        
        ASSERT_TRUE(nodes != NULL);
        ASSERT_TRUE(nodes->size() == 0);
        ASSERT_TRUE(roots != NULL);
        ASSERT_TRUE(roots->size() == 0);        
        
        delete(graph);
}

TEST_F(graph_test, linked_list)
{
        txn_graph *graph;
        vector<graph_node*> *graph_nodes;
        vector<int> *graph_roots;
        vector<int> *edges;
        graph_node **my_nodes;
        uint32_t i, num_nodes;
        
        graph = new txn_graph();
        graph_nodes = graph->get_nodes();
        graph_roots = graph->get_roots();

        num_nodes = 4;
        my_nodes = (graph_node**)zmalloc(sizeof(graph_node*)*num_nodes);
        for (i = 0; i < num_nodes; ++i) {
                my_nodes[i] = new graph_node();
                graph->add_node(my_nodes[i]);
        }

        EXPECT_TRUE(graph_nodes->size() == num_nodes);
        EXPECT_TRUE(graph_roots->size() == num_nodes);
        EXPECT_TRUE(graph_roots == graph->get_roots());
        
        for (i = 0; i < num_nodes; ++i) {                 
                (*graph_roots)[i] = 1;
                EXPECT_TRUE(find_node<graph_node*>(my_nodes[i], graph_nodes) >= 0);
        }

        for (i = 0; i < num_nodes-1; ++i) 
                graph->add_edge(my_nodes[i], my_nodes[i+1]);
        
        EXPECT_TRUE(graph_nodes->size() == 4);
        EXPECT_TRUE(graph_roots->size() == 4);

        for (i = 0; i < num_nodes; ++i) {

                if (i == 0) {

                        EXPECT_TRUE((*graph_roots)[i] == 1);
                        edges = my_nodes[i]->in_links;
                        EXPECT_TRUE(edges == NULL);
                        edges = my_nodes[i]->out_links;
                        EXPECT_TRUE(edges != NULL);
                        EXPECT_TRUE(edges->size() == 1);
                        EXPECT_TRUE((*edges)[0] == 1);

                } else if (i == 1 || i == 2) {
                        
                        EXPECT_TRUE((*graph_roots)[i] == 0);
                        edges = my_nodes[i]->in_links;
                        EXPECT_TRUE(edges != NULL);
                        EXPECT_TRUE(edges->size() == 1);
                        EXPECT_TRUE((*edges)[0] == i-1);
                        
                        edges = my_nodes[i]->out_links;
                        EXPECT_TRUE(edges != NULL);
                        EXPECT_TRUE(edges->size() == 1);
                        EXPECT_TRUE((*edges)[0] == i+1);
                } else if (i == 3) {
                        EXPECT_TRUE((*graph_roots)[i] == 0);
                        edges = my_nodes[i]->in_links;
                        EXPECT_TRUE(edges != NULL);
                        EXPECT_TRUE(edges->size() == 1);
                        EXPECT_TRUE((*edges)[0] == 2);
                        
                        edges = my_nodes[i]->out_links;
                        EXPECT_TRUE(edges == NULL);
                } 
        }
        
        graph->add_edge(my_nodes[3], my_nodes[0]);
        for (i = 0; i < num_nodes; ++i) 
                EXPECT_TRUE((*graph_roots)[i] == 0);
        delete(graph);
}

TEST_F(graph_test, complex_test)
{
        graph_node **my_nodes;
        txn_graph *graph;
        vector<graph_node*> *nodes;
        vector<int> *edges;
        vector<int> *roots;
        uint32_t i, num_nodes;

        /* Setup complex graph */
        num_nodes = 6;
        my_nodes = (graph_node**)zmalloc(sizeof(graph_node*)*num_nodes);
        graph = new txn_graph();
        
        /* Setup nodes */
        for (i = 0; i < num_nodes; ++i) {
                my_nodes[i] = new graph_node();
                graph->add_node(my_nodes[i]);
        }
        
        /* Setup edges */
        graph->add_edge(my_nodes[0], my_nodes[1]);
        graph->add_edge(my_nodes[0], my_nodes[2]);
        graph->add_edge(my_nodes[1], my_nodes[3]);
        graph->add_edge(my_nodes[2], my_nodes[3]);
        graph->add_edge(my_nodes[4], my_nodes[3]);
        graph->add_edge(my_nodes[5], my_nodes[3]);
        
        /* Test nodes */
        nodes = graph->get_nodes();
        EXPECT_TRUE(nodes->size() == num_nodes);
        roots = graph->get_roots();
        EXPECT_TRUE(roots->size() == num_nodes);
        for (i = 0; i < num_nodes; ++i) {
                if (i == 0 || i == 4 || i == 5)
                        EXPECT_TRUE((*roots)[i] == 1);
                else
                        EXPECT_TRUE((*roots)[i] == 0);
        }

        /* Test edges */
        for (i = 0; i < num_nodes; ++i) {
                if (i == 0) {
                        edges = (*nodes)[i]->in_links;
                        EXPECT_TRUE(edges == NULL);
                        
                        edges = (*nodes)[i]->out_links;
                        EXPECT_TRUE(edges->size() == 2);
                        EXPECT_TRUE(find_node<int>(0, edges) == -1);
                        EXPECT_TRUE(find_node<int>(1, edges) >= 0);
                        EXPECT_TRUE(find_node<int>(2, edges) >= 0);
                        EXPECT_TRUE(find_node<int>(3, edges) == -1);
                        EXPECT_TRUE(find_node<int>(4, edges) == -1);
                        EXPECT_TRUE(find_node<int>(5, edges) == -1);
                } else if (i == 1 || i == 2) {
                        edges = (*nodes)[i]->in_links;
                        EXPECT_TRUE(edges->size() == 1);
                        EXPECT_TRUE(find_node<int>(0, edges) >= 0);
                        EXPECT_TRUE(find_node<int>(1, edges) == -1);
                        EXPECT_TRUE(find_node<int>(2, edges) == -1);
                        EXPECT_TRUE(find_node<int>(3, edges) == -1);
                        EXPECT_TRUE(find_node<int>(4, edges) == -1);
                        EXPECT_TRUE(find_node<int>(5, edges) == -1);
                        
                        edges = (*nodes)[i]->out_links;
                        EXPECT_TRUE(edges->size() == 1);
                        EXPECT_TRUE(find_node<int>(0, edges) == -1);
                        EXPECT_TRUE(find_node<int>(1, edges) == -1);
                        EXPECT_TRUE(find_node<int>(2, edges) == -1);
                        EXPECT_TRUE(find_node<int>(3, edges) >= 0);
                        EXPECT_TRUE(find_node<int>(4, edges) == -1);
                        EXPECT_TRUE(find_node<int>(5, edges) == -1);
                } else if (i == 3) {
                        edges = (*nodes)[i]->in_links;
                        EXPECT_TRUE(edges->size() == 4);
                        EXPECT_TRUE(find_node<int>(0, edges) == -1);
                        EXPECT_TRUE(find_node<int>(1, edges) >= 0);
                        EXPECT_TRUE(find_node<int>(2, edges) >= 0);
                        EXPECT_TRUE(find_node<int>(3, edges) == -1);
                        EXPECT_TRUE(find_node<int>(4, edges) >= 0);
                        EXPECT_TRUE(find_node<int>(5, edges) >= 0);
                        
                        edges = (*nodes)[i]->out_links;
                        EXPECT_TRUE(edges == NULL);
                } else if (i == 4 || i == 5) {
                        edges = (*nodes)[i]->in_links;
                        EXPECT_TRUE(edges == NULL);

                        edges = (*nodes)[i]->out_links;
                        EXPECT_TRUE(edges->size() == 1);
                        EXPECT_TRUE(find_node<int>(0, edges) == -1);
                        EXPECT_TRUE(find_node<int>(1, edges) == -1);
                        EXPECT_TRUE(find_node<int>(2, edges) == -1);
                        EXPECT_TRUE(find_node<int>(3, edges) >= 0);
                        EXPECT_TRUE(find_node<int>(4, edges) == -1);
                        EXPECT_TRUE(find_node<int>(5, edges) == -1);
                }
        }
        delete(graph);
}

TEST_F(graph_test, single_topological_sort)
{
        graph_node *node;
        txn_graph *graph;
        vector<split_action*> *topo_sort;

        graph = new txn_graph();
        node = new graph_node();
        node->txn = (void*)(uint64_t)3;
        graph->add_node(node);
        
        topo_sort = new vector<split_action*>();
        setup_split::gen_piece_array(graph, topo_sort);
        
        ASSERT_TRUE(topo_sort->size() == 1);
        ASSERT_TRUE((*topo_sort)[0] == (split_action*)(uint64_t)3);
        
        delete(graph);
        topo_sort->clear();
        
        graph = new txn_graph();
        setup_split::gen_piece_array(graph, topo_sort);
        ASSERT_TRUE(topo_sort->size() == 0);        
}

TEST_F(graph_test, linked_topological_sort)
{
        uint32_t i, num_nodes, *node_indices;
        graph_node **my_nodes;
        txn_graph *graph;        
        vector<split_action*> *topo_sort;
        int index;
        
        /* Setup linked-list graph */
        num_nodes = 4;
        my_nodes = (graph_node**)zmalloc(sizeof(graph_node*)*num_nodes);
        graph = new txn_graph();
        
        /* Setup nodes */
        for (i = 0; i < num_nodes; ++i) {
                my_nodes[i] = new graph_node();
                my_nodes[i]->txn = (void*)(uint64_t)i;
                graph->add_node(my_nodes[i]);
        }
        
        /* Setup edges */
        graph->add_edge(my_nodes[0], my_nodes[1]);
        graph->add_edge(my_nodes[1], my_nodes[2]);
        graph->add_edge(my_nodes[2], my_nodes[3]);
        
        /* Do topological sort */
        topo_sort = new vector<split_action*>();
        setup_split::gen_piece_array(graph, topo_sort);
        
        /* Verify results */
        node_indices = (uint32_t*)zmalloc(sizeof(uint32_t)*num_nodes);
        for (i = 0; i < num_nodes; ++i) {
                index = find_node((split_action*)(uint64_t)i, topo_sort);
                ASSERT_TRUE(index != -1);
                node_indices[i] = (uint32_t)index;
        }
        
        ASSERT_TRUE(node_indices[0] < node_indices[1]);
        ASSERT_TRUE(node_indices[1] < node_indices[2]);
        ASSERT_TRUE(node_indices[2] < node_indices[3]);        
        
        /* Create a cycle */
        graph->add_edge(my_nodes[3], my_nodes[0]);
        topo_sort->clear();
        ASSERT_TRUE(topo_sort->size() == 0);
        ASSERT_DEATH(setup_split::gen_piece_array(graph, topo_sort), "");
}

TEST_F(graph_test, complex_topological_sort)
{
        uint32_t i, num_nodes, *node_indices;
        graph_node **my_nodes;
        txn_graph *graph;        
        vector<split_action*> *topo_sort;
        int index;

        /* Setup complex graph */
        num_nodes = 6;
        my_nodes = (graph_node**)zmalloc(sizeof(graph_node*)*num_nodes);
        graph = new txn_graph();
        
        /* Setup nodes */
        for (i = 0; i < num_nodes; ++i) {
                my_nodes[i] = new graph_node();
                my_nodes[i]->txn = (void*)(uint64_t)i;
                graph->add_node(my_nodes[i]);
        }
        
        /* Setup edges */
        graph->add_edge(my_nodes[0], my_nodes[1]);
        graph->add_edge(my_nodes[0], my_nodes[2]);
        graph->add_edge(my_nodes[1], my_nodes[3]);
        graph->add_edge(my_nodes[2], my_nodes[3]);
        graph->add_edge(my_nodes[4], my_nodes[3]);
        graph->add_edge(my_nodes[5], my_nodes[3]);        
        
        /* Do topological sort */
        topo_sort = new vector<split_action*>();
        setup_split::gen_piece_array(graph, topo_sort);
        
        /* Verify results */
        node_indices = (uint32_t*)zmalloc(sizeof(uint32_t)*num_nodes);
        for (i = 0; i < num_nodes; ++i) {
                index = find_node((split_action*)(uint64_t)i, topo_sort);
                ASSERT_TRUE(index != -1);
                node_indices[i] = (uint32_t)index;
        }
        
        ASSERT_TRUE(node_indices[0] < node_indices[1]);
        ASSERT_TRUE(node_indices[0] < node_indices[2]);
        ASSERT_TRUE(node_indices[1] < node_indices[3]);
        ASSERT_TRUE(node_indices[2] < node_indices[3]);
        ASSERT_TRUE(node_indices[4] < node_indices[3]);
        ASSERT_TRUE(node_indices[5] < node_indices[3]);
        
        delete(topo_sort);
        free(node_indices);
        free(my_nodes);
        delete(graph);
}

TEST_F(graph_test, complex_rvp)
{
        simple_split **actions;
        simple_split *cur_action;
        graph_node **nodes;
        vector<uint64_t> keys;
        vector<split_action*> split_actions;
        split_action **ordered_splits;
        uint32_t i, num_actions;
        txn_graph *graph;

        /* Setup nodes */
        num_actions = 6;
        actions = (simple_split**)zmalloc(sizeof(split_action*)*num_actions);
        nodes = (graph_node**)zmalloc(sizeof(split_action*)*num_actions);
        ordered_splits = (split_action**)zmalloc(sizeof(split_action*)*num_actions);
        graph = new txn_graph();
        for (i = 0; i < num_actions; ++i) {
                actions[i] = new simple_split(keys);
                nodes[i] = new graph_node();
                nodes[i]->app = actions[i];
                graph->add_node(nodes[i]);                
        }
        
        /* Add edges */        
        graph->add_edge(nodes[0], nodes[1]);
        graph->add_edge(nodes[0], nodes[2]);
        graph->add_edge(nodes[1], nodes[3]);
        graph->add_edge(nodes[2], nodes[3]);
        graph->add_edge(nodes[4], nodes[3]);
        graph->add_edge(nodes[5], nodes[3]);
        
        /* Process the graph */
        setup_split::graph_to_txn(graph, &split_actions);
        
        ASSERT_TRUE(split_actions.size() == num_actions);

        /* Check that RVPs have been correctly generated */
        for (i = 0; i < num_actions; ++i) {
                cur_action = (simple_split*)get_app(split_actions[i]);
                if (cur_action == actions[0]) {
                        ASSERT_TRUE(get_rvp_count(split_actions[i]) == 1);
                        ordered_splits[0] = split_actions[i];
                } else if (cur_action == actions[1]) {
                        ASSERT_TRUE(get_rvp_count(split_actions[i]) == 1);
                        ordered_splits[1] = split_actions[i];
                } else if (cur_action == actions[2]) {
                        ASSERT_TRUE(get_rvp_count(split_actions[i]) == 1);
                        ordered_splits[2] = split_actions[i];
                } else if (cur_action == actions[3]) {
                        ASSERT_TRUE(get_rvp_count(split_actions[i]) == 0);
                        ordered_splits[3] = split_actions[i];
                } else if (cur_action == actions[4]) {
                        ASSERT_TRUE(get_rvp_count(split_actions[i]) == 1);
                        ordered_splits[4] = split_actions[i];
                } else if (cur_action == actions[5]) {
                        ASSERT_TRUE(get_rvp_count(split_actions[i]) == 1);
                        ordered_splits[5] = split_actions[i];
                }                        
        }

        EXPECT_TRUE(get_rvps(ordered_splits[1])[0] == 
                    get_rvps(ordered_splits[2])[0]);
        EXPECT_TRUE(get_rvps(ordered_splits[1])[0] == 
                    get_rvps(ordered_splits[4])[0]);
        EXPECT_TRUE(get_rvps(ordered_splits[1])[0] == 
                    get_rvps(ordered_splits[5])[0]);

        EXPECT_TRUE(find_rvp(ordered_splits[1], get_rvps(ordered_splits[0])[0]));
        EXPECT_TRUE(find_rvp(ordered_splits[2], get_rvps(ordered_splits[0])[0]));
        EXPECT_TRUE(find_rvp(ordered_splits[3], get_rvps(ordered_splits[1])[0]));
        EXPECT_TRUE(find_rvp(ordered_splits[3], get_rvps(ordered_splits[2])[0]));
        EXPECT_TRUE(find_rvp(ordered_splits[3], get_rvps(ordered_splits[4])[0]));
        EXPECT_TRUE(find_rvp(ordered_splits[3], get_rvps(ordered_splits[5])[0]));
}

TEST_F(graph_test, linked_rvp)
{
        simple_split **actions;
        simple_split *cur_action;
        graph_node **nodes;
        vector<uint64_t> keys;
        vector<split_action*> split_actions;
        uint32_t i, num_actions;
        txn_graph *graph;

        graph = new txn_graph();
        num_actions = 4;
        actions = (simple_split**)zmalloc(sizeof(simple_split*)*num_actions);
        nodes = (graph_node**)zmalloc(sizeof(graph_node*)*num_actions);
        for (i = 0; i < num_actions; ++i) {
                actions[i] = new simple_split(keys);
                nodes[i] = new graph_node();
                nodes[i]->app = actions[i];
                graph->add_node(nodes[i]);
        }

        graph->add_edge(nodes[0], nodes[1]);
        graph->add_edge(nodes[1], nodes[2]);
        graph->add_edge(nodes[2], nodes[3]);

        setup_split::graph_to_txn(graph, &split_actions);
        ASSERT_TRUE(split_actions.size() == 4);
        
        /* Check that RVPs have been correctly generated */
        for (i = 0; i < num_actions; ++i) {
                cur_action = (simple_split*)get_app(split_actions[i]);
                if (cur_action == actions[num_actions-1])
                        ASSERT_TRUE(get_rvp_count(split_actions[i]) == 0);
                else 
                        ASSERT_TRUE(get_rvp_count(split_actions[i]) == 1);
        }
        
        ASSERT_TRUE(find_rvp(split_actions[1], get_rvps(split_actions[0])[0]));
        ASSERT_TRUE(find_rvp(split_actions[2], get_rvps(split_actions[1])[0]));
        ASSERT_TRUE(find_rvp(split_actions[3], get_rvps(split_actions[2])[0]));
        
        free(nodes);
        delete(graph);
        free(actions);                
}

