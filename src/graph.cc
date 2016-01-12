#include <graph.h>
#include <cstddef>
#include <cpuinfo.h>
#include <cassert>

node_set::node_set(uint32_t sz)
{        
        this->sz = sz;
        if (sz <= 64) {
                array_mask = NULL;
                bit_mask = 0;
        } else {
                array_mask = (char*)zmalloc(sizeof(char)*sz);                
        }
}

node_set::~node_set()
{
        if (sz > 64)
                free(array_mask);
}

void node_set::add_node(int index)
{
        if (sz <= 64) {
                assert(index < 64);
                bit_mask |= (((uint64_t)1) << index);
        } else {
                array_mask[index] = 1;
        }
}

bool node_set::operator==(const node_set& other)
{
        /* Nodes from the same graph have sets of the same size */
        assert(sz == other.sz);
        
        uint32_t i, sz;
        
        if (sz <= 64)
                return bit_mask == other.bit_mask;

        for (i = 0; i < sz; ++i) 
                if (array_mask[i] != other.array_mask[i])
                        return false;
        return true;
}

bool node_set::operator!=(const node_set& other)
{
        return !(*this == other);
}


txn_graph::txn_graph()
{
        root_bitmap = new vector<int>();
        nodes = new vector<graph_node*>();
}

vector<int>* txn_graph::get_roots()
{
        return root_bitmap;
}

vector<graph_node*>* txn_graph::get_nodes()
{
        return nodes;
}

void txn_graph::add_node(graph_node *node)
{
        /* Edges cannot be added before the node is added to the graph */
        assert(node->in_links == NULL && node->out_links == NULL);
        assert(node->index == INT_MAX);
        assert(root_bitmap->size() == nodes->size());
        
        node->index = nodes->size();
        root_bitmap->push_back(1);
        nodes->push_back(node);
}

void txn_graph::add_edge(graph_node *from, graph_node *to)
{
        /* Nodes should have already been inserted */
        assert(from->index != INT_MAX && to->index != INT_MAX);

        if (from->out_links == NULL) {
                from->out_links = new vector<int>();
        } 
        if (to->in_links == NULL) {
                to->in_links = new vector<int>();
        }
        
        from->out_links->push_back(to->index);
        to->in_links->push_back(from->index);
}

txn_graph::~txn_graph()
{
        graph_node *cur_node;
        uint32_t i, num_nodes;
        
        num_nodes = nodes->size();
        for (i = 0; i < num_nodes; ++i) {
                cur_node = (*nodes)[i];
                delete(cur_node->out_links);
                delete(cur_node->in_links);
                cur_node->index = INT_MAX;
                delete(cur_node);
        }
        delete(root_bitmap);
        delete(nodes);
}
