#ifndef 	GRAPH_H_
#define 	GRAPH_H_

#include <stdint.h>
#include <vector>
#include <limits.h>
#include <cstddef>

using namespace std;

/* Represents a set of graph nodes */
class node_set {
 private:
        uint32_t sz;
        uint64_t bit_mask;
        char *array_mask;

 public:
        node_set(uint32_t sz);
        ~node_set();

        void add_node(int index);
        bool operator==(const node_set& other);
        bool operator!=(const node_set& other);
};

class graph_node {
 public:
        int index;
        void *app;        
        void *txn;

        vector<int> *in_links;
        vector<int> *out_links;

        graph_node* topo_link;

        graph_node() {
                index = INT_MAX;
                app = NULL;
                txn = NULL;
                in_links = NULL;
                out_links = NULL;
                topo_link = NULL;
        }
};

class txn_graph {
 private:
        vector<int> *root_bitmap;
        vector<graph_node*>* nodes;

 public:        
        txn_graph();
        ~txn_graph();
        vector<int>* get_roots();
        vector<graph_node*>* get_nodes();
        void add_node(graph_node *node);
        void add_edge(graph_node *from, graph_node *to);
};

#endif 		// GRAPH_H_
