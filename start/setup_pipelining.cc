/*

#include <setup_pipelining.h>
#include <pipelined_executor.h>
#include <pipelined_action.h>


static pipelined::action_batch setup_db(workload_config w_conf)
{
        txn **loader_txns;
        uint32_t num_txns, i;
        pipelined::action_batch ret;

        loader_txns = NULL;
        num_txns = generate_input(w_conf, &loader_txns);
        assert(loader_txns != NULL);
        ret.batchSize = num_txns;
        ret.batch = (locking_action**)malloc(sizeof(locking_action*)*num_txns);
        assert(ret.batch != NULL);
        for (i = 0; i < num_txns; ++i) 
                ret.batch[i] = txn_to_action(loader_txns[i]);
        return ret;
}


void pipelining_experiment(locking_config conf, workload_config w_conf)
{
        pipelined::txn_queue **input, **output;
        pipelined::action_batch **experiment_txns, setup_txns;
        
        table_mgr *tables;
        uint32_t num_tables, *num_records;
        struct locking_result result;
        LockManager *lock_manager;
        pipelined_executors **workers;

        tpcc_config::num_warehouses = w_conf.num_warehouses;
        inputs = setup_queues<pipelined::action_batch>(conf.num_threads, 1024);
        outputs = setup_queues<pipelined::action_batch>(conf.num_threads, 1024);
        setup_txns = setup_db(w_conf);
        experiment_txns = setup_input(conf, w_conf, EXTRA_BATCHES);
        
        assert(w_conf.experiment == TPCC_SUBSET);
        assert(conf.num_threads > 0);
        
        tables = setup_hash_tables(w_conf, PIPELINED);
        lock_manager = new LockManager(tables);        
        workers = setup_workers(inputs, outputs, lock_manager,
                                conf.num_threads, 50, tables, num_tables, w_conf);
        result = do_measurement(conf, workers, inputs, outputs, experiment_txns,
                                1+EXTRA_BATCHES, setup_txns, tables);
        write_output(conf, result, w_conf);
}
*/
