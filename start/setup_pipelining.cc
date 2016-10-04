#include <setup_pipelining.h>
#include <pipelined_executor.h>
#include <pipelined_action.h>
#include <setup_workload.h>
#include <pipelined_tpcc.h>
#include <uniform_generator.h>
#include <common.h>
#include <lock_manager.h>
#include <iostream>
#include <fstream>

#define P_EXTRA_BATCHES 1 

using namespace pipelined;

extern RecordBuffersConfig setup_buffer_config(int cpu, workload_config w_conf);
extern locking_action* txn_to_locking_action(txn *t);
extern uint64_t gen_unique_key(RecordGenerator *gen, std::set<uint64_t> *seen_keys);

struct pipelining_result {
        double time;
        timespec elapsed_time;
        uint64_t num_txns;
};


static action* locking_action_to_action(locking_action **lck_txns, uint32_t type)
{
        if (type == NEW_ORDER_TXN) 
                return new action(type, lck_txns, 6);
        else if (type == PAYMENT_TXN) 
                return new action(type, lck_txns, 4);
        else if (type == LOADER_TXN) 
                return new action(type, lck_txns, 1);
        else 
                assert(false);
        
        return NULL;
}

static action_batch setup_db(workload_config w_conf)
{
        txn **loader_txns;
        uint32_t num_txns, i;
        action_batch ret;
        locking_action *lck_txn, **txn_ptrs;
        action *p_action;

        loader_txns = NULL;
        num_txns = generate_input(w_conf, &loader_txns);
        assert(loader_txns != NULL);
        ret._batch_sz = num_txns;
        ret._txns = (action**)malloc(sizeof(action*)*num_txns);
        assert(ret._txns != NULL);
        for (i = 0; i < num_txns; ++i) {
                txn_ptrs = (locking_action**)zmalloc(sizeof(locking_action*));
                txn_ptrs[0] = txn_to_locking_action(loader_txns[i]);
                ret._txns[i] = locking_action_to_action(txn_ptrs, LOADER_TXN);
        }
        return ret;
}

static action* setup_payment(workload_config w_conf, uint32_t thread_id)
{
        uint32_t w_id, d_id, c_id, c_w_id, c_d_id, time; 
        float h_amount;
        locking_action **lck_actions;
        p_payment::warehouse_update *w_txn;
        p_payment::district_update *d_txn;
        p_payment::customer_update *c_txn;
        p_payment::history_ins *h_txn;
        
        //        assert(thread_id < w_conf.num_warehouses);
        //        w_id = thread_id;
        w_id = (uint64_t)rand() % w_conf.num_warehouses;
        assert(w_id < w_conf.num_warehouses);

        d_id = (uint32_t)rand() % NUM_DISTRICTS;
        assert(d_id < NUM_DISTRICTS);
        
        c_id = (uint32_t)rand() % NUM_CUSTOMERS;
        assert(c_id < NUM_CUSTOMERS);

        c_w_id = w_id;
        c_d_id = d_id;

        /*
        temp = (uint32_t)rand() % 100;
        if (temp >= 15) {
                c_w_id = w_id;
                c_d_id = d_id;
        } else {
                c_d_id = (uint32_t)rand() % NUM_DISTRICTS;
                do {
                        c_w_id = (uint32_t)rand() % conf.num_warehouses;
                } while (c_w_id == w_id && conf.num_warehouses > 1);
        }
        */
        time = 0;
        h_amount = 1.0*((uint32_t)rand() % 5000);
        
        w_txn = new p_payment::warehouse_update(w_id, h_amount);
        d_txn = new p_payment::district_update(w_id, d_id, h_amount);
        c_txn = new p_payment::customer_update(w_id, d_id, c_w_id, c_d_id, c_id,
                                               h_amount);
        h_txn = new p_payment::history_ins(w_id, d_id, c_w_id, c_d_id, c_id, 
                                           time, 
                                           h_amount, 
                                           w_txn, 
                                           d_txn);
        
        lck_actions = (locking_action**)zmalloc(sizeof(locking_action*)*4);
        lck_actions[0] = txn_to_locking_action(w_txn);
        lck_actions[1] = txn_to_locking_action(d_txn);
        lck_actions[2] = txn_to_locking_action(c_txn);
        lck_actions[3] = txn_to_locking_action(h_txn);
        return new action(PAYMENT_TXN, lck_actions, 4);
}

static action* setup_new_order(workload_config w_conf, uint32_t thread_id)
{
        uint32_t w_id, d_id, c_id, *quants, nitems, i, temp;
        uint64_t *suppliers, *items;
        UniformGenerator item_gen(NUM_ITEMS);
        set<uint64_t> seen_items;
        action *ret;
        locking_action **lck_txns;
        txn *temp_txn; 
        p_new_order::warehouse_read *wh_txn;
        p_new_order::district_update *d_txn;
        p_new_order::customer_read *c_txn;
        bool all_local;

        all_local = true;
        //        assert(thread_id < w_conf.num_warehouses);
        //        w_id = thread_id;
        w_id = (uint64_t)rand() % w_conf.num_warehouses;
        assert(w_id < w_conf.num_warehouses);
        
        d_id = (uint32_t)rand() % NUM_DISTRICTS;
        assert(d_id < NUM_DISTRICTS);
        
        c_id = (uint32_t)rand() % NUM_CUSTOMERS;
        assert(c_id < NUM_CUSTOMERS);
        
        nitems = 5 + ((uint32_t)rand() % 11);
        items = (uint64_t*)zmalloc(sizeof(uint64_t)*nitems);
        quants = (uint32_t*)zmalloc(sizeof(uint32_t)*nitems);
        suppliers = (uint64_t*)zmalloc(sizeof(uint64_t)*nitems);
        
        for (i = 0; i < nitems; ++i) {
                items[i] = gen_unique_key(&item_gen, &seen_items);
                quants[i] = 1 + ((uint32_t)rand() % 10);
                //                suppliers[i] = w_id;
                temp = rand() % 100;
                if (temp == 0) {                        
                        do {
                                suppliers[i] = rand() % w_conf.num_warehouses;
                        } while (suppliers[i] == w_id && w_conf.num_warehouses > 1);
                        all_local = (w_conf.num_warehouses > 1);
                } else {

                        suppliers[i] = w_id;
                }
        }
        
        lck_txns = (locking_action**)zmalloc(sizeof(locking_action*)*6);
        
        /* Warehouse */
        wh_txn = new p_new_order::warehouse_read(w_id);        
        lck_txns[0] = txn_to_locking_action(wh_txn);
        
        /* District */        
        d_txn = new p_new_order::district_update(w_id, d_id);
        lck_txns[1] = txn_to_locking_action(d_txn);

        /* Customer */
        c_txn = new p_new_order::customer_read(w_id, d_id, c_id);
        lck_txns[2] = txn_to_locking_action(c_txn);


        temp_txn = new p_new_order::process_items(w_id, d_id, suppliers, items, 
                                                  quants, 
                                                  nitems, 
                                                  wh_txn,
                                                  d_txn, 
                                                  c_txn);
        lck_txns[3] = txn_to_locking_action(temp_txn);
        

        temp_txn = new p_new_order::new_order_ins(w_id, d_id, d_txn);
        lck_txns[4] = txn_to_locking_action(temp_txn);
                

        temp_txn = new p_new_order::oorder_ins(w_id, d_id, c_id, nitems, 
                                               all_local, 
                                               d_txn);
        lck_txns[5] = txn_to_locking_action(temp_txn);
        
        /* Create a pipelined action */
        return new action(NEW_ORDER_TXN, lck_txns, 6);
}


static action_batch create_single_batch(uint32_t num_txns, 
                                        workload_config w_conf, 
                                        uint32_t thread_id)
{
        assert(w_conf.experiment == TPCC_SUBSET);
        uint32_t i;
        action **action_array;
        action_batch ret;

        action_array = (action**)zmalloc(sizeof(action*)*num_txns);        
        for (i = 0; i < num_txns; ++i) {
                if (rand() % 2 == 0) 
                        action_array[i] = setup_new_order(w_conf, thread_id);
                else 
                        action_array[i] = setup_payment(w_conf, thread_id);
        }
        ret._batch_sz = num_txns;
        ret._txns = action_array;
        return ret;
}

static action_batch* setup_single_round(uint32_t num_txns, uint32_t num_threads,
                                        workload_config w_conf)
{
        action_batch *ret;
        uint32_t i, txns_per_thread, remainder;

        ret = (action_batch*)malloc(sizeof(action_batch)*num_threads);
        txns_per_thread = num_txns / num_threads;
        remainder = num_txns % num_threads;
        for (i = 0; i < num_threads; ++i) {
                if (i < remainder)
                        ret[i] = create_single_batch(txns_per_thread+1, w_conf, i);
                else
                        ret[i] = create_single_batch(txns_per_thread, w_conf, i);
        }
        return ret;

}

static action_batch** setup_input(locking_config conf, workload_config w_conf, 
                                  uint32_t extra_batches)
{
        assert(w_conf.experiment == TPCC_SUBSET);
        action_batch **ret;
        uint32_t i, total_iters;
        
        total_iters = 2 + extra_batches;
        ret = (action_batch**)zmalloc(sizeof(action_batch*)*total_iters);
        ret[0] = setup_single_round(FAKE_ITER_SIZE*conf.num_threads, conf.num_threads, w_conf);
        ret[1] = setup_single_round(conf.num_txns, conf.num_threads, w_conf);
        for (i = 2; i < total_iters; ++i) 
                ret[i] = setup_single_round(conf.num_txns, conf.num_threads, w_conf);

        return ret;
}

static executor** setup_workers(txn_queue **inputs, txn_queue **outputs, 
                               LockManager *lck_mgr, 
                               uint32_t nthreads,
                               table_mgr *tbl_mgr,
                               workload_config w_conf)
{
        assert(lck_mgr != NULL && tbl_mgr != NULL);
        executor **ret;
        int i;
        
        ret = (executor**)zmalloc(sizeof(executor*)*nthreads);
        for (i = 0; i < nthreads; ++i) {
                struct executor_config conf = {
                        tbl_mgr,
                        lck_mgr,
                        i,
                        inputs[i],
                        outputs[i],
                };
                
                struct RecordBuffersConfig rb_conf = setup_buffer_config(i, w_conf);
                ret[i] = new(i) executor(conf, rb_conf);
        }        
        return ret;
}

static struct pipelining_result do_measurement(locking_config conf,
                                               executor **workers,
                                               txn_queue **inputs,
                                               txn_queue **outputs,
                                               action_batch **batches,
                                               uint32_t num_batches,
                                               action_batch setup,
                                               table_mgr *tbl_mgr)
{
        uint32_t i, j;
        struct pipelining_result result;
        timespec start_time, end_time;

        pin_thread(79);
        
        /* Start worker threads. */
        for (i = 0; i < conf.num_threads; ++i) {
                workers[i]->Run();
                workers[i]->WaitInit();
        }

        /* Setup the database. */
        inputs[0]->EnqueueBlocking(setup);
        outputs[0]->DequeueBlocking();
        tbl_mgr->set_init();
        
        //        for (i = 0; i < num_tables; ++i)
        //                tables[i]->SetInit();

        std::cerr << "Done setting up tables!\n";
        
       /* Dry run */
        for (i = 0; i < conf.num_threads; ++i) {
                inputs[i]->EnqueueBlocking(batches[0][i]);
        }
        for (i = 0; i < conf.num_threads; ++i) {
                outputs[i]->DequeueBlocking();
        }

        std::cerr << "Done with dry run!\n";
        
        //        double start_dbl = GetTime();
        barrier();
        clock_gettime(CLOCK_REALTIME, &start_time);
        barrier();

        for (i = 1; i < num_batches; ++i)
                for (j = 0; j < conf.num_threads; ++j)
                        inputs[j]->EnqueueBlocking(batches[i][j]);
        for (i = 0; i < conf.num_threads; ++i)
                outputs[i]->DequeueBlocking();
        //        double end_dbl = GetTime();
        barrier();
        clock_gettime(CLOCK_REALTIME, &end_time);
        barrier();
        //        result.time = end_dbl - start_dbl;
        result.elapsed_time = diff_time(end_time, start_time);
        return result;
}

static void write_output(locking_config conf, struct pipelining_result result,
                         workload_config w_conf)
{
        std::ofstream result_file;
        double elapsed_milli;
        timespec elapsed_time;

        elapsed_time = result.elapsed_time;
        elapsed_milli =
                1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
        result_file.open("pipelined.txt", std::ios::app | std::ios::out);
        result_file << "pipelined ";
        result_file << "time:" << elapsed_milli << " ";
        result_file << "txns:" << conf.num_txns << " ";
        result_file << "threads:" << conf.num_threads << " ";
        result_file << "records:" << conf.num_records << " ";
        result_file << "read_pct:" << conf.read_pct << " ";
        result_file << "txn_size:" << w_conf.txn_size << " ";
        if (conf.experiment == 0) 
                result_file << "10rmw" << " ";
        else if (conf.experiment == 1) 
                result_file << "8r2rmw" << " ";
        else if (conf.experiment == 3) 
                result_file << "small_bank" << " ";
        else if (conf.experiment == TPCC_SUBSET)
                result_file << "tpcc" << " ";
 
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

void pipelining_experiment(locking_config conf, workload_config w_conf)
{
        txn_queue **inputs, **outputs;
        action_batch **experiment_txns, setup_txns;
        
        table_mgr *tables;
        uint32_t num_tables, *num_records;
        struct pipelining_result result;
        LockManager *lock_manager;
        executor **workers;

        tpcc_config::num_warehouses = w_conf.num_warehouses;
        inputs = setup_queues<pipelined::action_batch>(conf.num_threads, 1024);
        outputs = setup_queues<pipelined::action_batch>(conf.num_threads, 1024);
        setup_txns = setup_db(w_conf);
        experiment_txns = setup_input(conf, w_conf, P_EXTRA_BATCHES);
        
        assert(w_conf.experiment == TPCC_SUBSET);
        assert(conf.num_threads > 0);
        
        tables = setup_hash_tables(w_conf, PIPELINED);
        lock_manager = new LockManager(tables);        

        workers = setup_workers(inputs, outputs, lock_manager, conf.num_threads,
                                tables, 
                                w_conf);

        result = do_measurement(conf, workers, inputs, outputs, experiment_txns,
                                1+P_EXTRA_BATCHES, setup_txns, tables);
        write_output(conf, result, w_conf);
}
