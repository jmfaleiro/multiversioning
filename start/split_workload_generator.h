#ifndef 	SPLIT_WORKLOAD_GENERATOR_H_
#define 	SPLIT_WORKLOAD_GENERATOR_H_

#include <graph.h>

txn_graph* generate_split_action(workload_config conf, uint32_t num_partitions);

#endif 		// SPLIT_WORKLOAD_GENERATOR_H_
