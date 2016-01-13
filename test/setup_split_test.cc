#include <gtest/gtest.h>
#include <graph.h>
#include <iostream>
#include <cpuinfo.h>

struct node_internal {
        void *txn;
        void *app;
};

class graph_test : public ::testing::Test {

private:
        node_internal *nodes;

protected:
        
        virtual void SetUp() {
                
        }
};

class setup_split_test : public ::testing::Test {

protected:
        virtual void SetUp() {

        }

};

template<class T>
static bool find_node(T node, vector<T> *node_set)
{
        uint32_t i, sz;
        
        sz = node_set->size();
        for (i = 0; i < sz; ++i) 
                if ((*node_set)[i] == node)
                        return true;
        return false;        
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
                EXPECT_TRUE(find_node<graph_node*>(my_nodes[i], graph_nodes));
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
                        EXPECT_TRUE(find_node<int>(0, edges) == false);
                        EXPECT_TRUE(find_node<int>(1, edges) == true);
                        EXPECT_TRUE(find_node<int>(2, edges) == true);
                        EXPECT_TRUE(find_node<int>(3, edges) == false);
                        EXPECT_TRUE(find_node<int>(4, edges) == false);
                        EXPECT_TRUE(find_node<int>(5, edges) == false);
                } else if (i == 1 || i == 2) {
                        edges = (*nodes)[i]->in_links;
                        EXPECT_TRUE(edges->size() == 1);
                        EXPECT_TRUE(find_node<int>(0, edges) == true);
                        EXPECT_TRUE(find_node<int>(1, edges) == false);
                        EXPECT_TRUE(find_node<int>(2, edges) == false);
                        EXPECT_TRUE(find_node<int>(3, edges) == false);
                        EXPECT_TRUE(find_node<int>(4, edges) == false);
                        EXPECT_TRUE(find_node<int>(5, edges) == false);
                        
                        edges = (*nodes)[i]->out_links;
                        EXPECT_TRUE(edges->size() == 1);
                        EXPECT_TRUE(find_node<int>(0, edges) == false);
                        EXPECT_TRUE(find_node<int>(1, edges) == false);
                        EXPECT_TRUE(find_node<int>(2, edges) == false);
                        EXPECT_TRUE(find_node<int>(3, edges) == true);
                        EXPECT_TRUE(find_node<int>(4, edges) == false);
                        EXPECT_TRUE(find_node<int>(5, edges) == false);
                } else if (i == 3) {
                        edges = (*nodes)[i]->in_links;
                        EXPECT_TRUE(edges->size() == 4);
                        EXPECT_TRUE(find_node<int>(0, edges) == false);
                        EXPECT_TRUE(find_node<int>(1, edges) == true);
                        EXPECT_TRUE(find_node<int>(2, edges) == true);
                        EXPECT_TRUE(find_node<int>(3, edges) == false);
                        EXPECT_TRUE(find_node<int>(4, edges) == true);
                        EXPECT_TRUE(find_node<int>(5, edges) == true);
                        
                        edges = (*nodes)[i]->out_links;
                        EXPECT_TRUE(edges == NULL);
                } else if (i == 4 || i == 5) {
                        edges = (*nodes)[i]->in_links;
                        EXPECT_TRUE(edges == NULL);

                        edges = (*nodes)[i]->out_links;
                        EXPECT_TRUE(edges->size() == 1);
                        EXPECT_TRUE(find_node<int>(0, edges) == false);
                        EXPECT_TRUE(find_node<int>(1, edges) == false);
                        EXPECT_TRUE(find_node<int>(2, edges) == false);
                        EXPECT_TRUE(find_node<int>(3, edges) == true);
                        EXPECT_TRUE(find_node<int>(4, edges) == false);
                        EXPECT_TRUE(find_node<int>(5, edges) == false);
                }
        }
        delete(graph);
}
