#include <setup_occ.h>
#include <config.h>
#include <common.h>
#include <set>
#include <small_bank.h>
#include <fstream>
#include <setup_workload.h>
#include <unistd.h>
#include <tpcc.h>

extern uint32_t GLOBAL_RECORD_SIZE;

static bool is_ycsb_exp(workload_config w_conf)
{
        bool ret;
        ret = (w_conf.experiment == YCSB_10RMW || 
               w_conf.experiment == YCSB_2RMW8R || 
               w_conf.experiment == YCSB_SINGLE_HOT || 
               w_conf.experiment == YCSB_UPDATE || 
               w_conf.experiment == YCSB_RW);
        return ret;
}

OCCAction* setup_occ_action(txn *txn)
{
        OCCAction *action;
        struct big_key *array;
        uint32_t num_reads, num_writes, num_rmws, max, i;
        
        action = new OCCAction(txn);
        txn->set_translator(action);
        num_reads = txn->num_reads();
        num_writes = txn->num_writes();
        num_rmws = txn->num_rmws();

        if (num_reads >= num_writes && num_reads >= num_rmws) 
                max = num_reads;
        else if (num_writes >= num_rmws)
                max = num_writes;
        else
                max = num_rmws;
        array = (struct big_key*)malloc(sizeof(struct big_key)*max);

        txn->get_reads(array);
        for (i = 0; i < num_reads; ++i) 
                action->add_read_key(array[i].table_id, array[i].key);
        txn->get_rmws(array);
        for (i = 0; i < num_rmws; ++i) 
                action->add_write_key(array[i].table_id, array[i].key, true);
        txn->get_writes(array);
        for (i = 0; i < num_writes; ++i) {
                action->add_write_key(array[i].table_id, array[i].key, false);
        }        
        free(array);
        return action;
}

OCCAction** create_single_occ_action_batch(uint32_t batch_size,
                                           workload_config w_config, 
                                           uint32_t thread)
{
        uint32_t i;
        OCCAction **ret;
        txn *txn;
        
        ret = (OCCAction**)alloc_mem(batch_size*sizeof(OCCAction*), 71);
        //        assert(ret != NULL);
        memset(ret, 0x0, batch_size*sizeof(OCCAction*));
        for (i = 0; i < batch_size; ++i) {
                txn = generate_transaction(w_config, thread);
                ret[i] = setup_occ_action(txn);
                ret[i]->create_inserts(20);
        }
        return ret;
}

OCCActionBatch** setup_occ_input(OCCConfig occ_config, workload_config w_conf,
                                 uint32_t extra_batches)
{
        OCCActionBatch **ret;
        uint32_t i, total_iters;
        OCCConfig fake_config;
        
        total_iters = 1 + 1 + extra_batches; // dry run + real run + extra
        fake_config = occ_config;
        fake_config.numTxns = FAKE_ITER_SIZE;
        ret = (OCCActionBatch**)malloc(sizeof(OCCActionBatch*)*(total_iters));
        ret[0] = setup_occ_single_input(fake_config, w_conf);
        ret[1] = setup_occ_single_input(occ_config, w_conf);
        occ_config.numTxns = 1000000;
        fake_config.numTxns = occ_config.numTxns;
        for (i = 2; i < total_iters; ++i) 
                ret[i] = setup_occ_single_input(fake_config, w_conf);
        std::cerr << "Done setting up occ input\n";
        return ret;
}

OCCActionBatch* setup_occ_single_input(OCCConfig config, workload_config w_conf)
{
        OCCActionBatch *ret;
        uint32_t txns_per_thread, remainder, i;
        OCCAction **actions;

        config.numThreads -= 1;
        ret = (OCCActionBatch*)malloc(sizeof(OCCActionBatch)*config.numThreads);
        txns_per_thread = (config.numTxns)/config.numThreads;
        remainder = (config.numTxns) % config.numThreads;
        for (i = 0; i < config.numThreads; ++i) {
                if (i == config.numThreads-1)
                        txns_per_thread += remainder;
                actions = create_single_occ_action_batch(txns_per_thread,
                                                         w_conf,
                                                         i);
                ret[i] = {
                        txns_per_thread,
                        actions,
                };
        }
        return ret;
}

Table** setup_occ_lock_tables(OCCConfig o_conf, workload_config w_conf)
{
        assert(READ_COMMITTED);
        char val[CACHE_LINE];
        uint32_t i, j, k;
        int start_cpu, end_cpu;
        Table **ret, *tbl;
        uint64_t num_warehouses, num_districts, num_customers, num_stocks;
        
        start_cpu = 1;
        end_cpu = o_conf.numThreads - 1;
        memset(val, 0x0, CACHE_LINE);

        /* First create a table */
        if (is_ycsb_exp(w_conf)) {
                TableConfig ycsb_conf = {
                        0,
                        2*w_conf.num_records,
                        start_cpu,
                        end_cpu,
                        2*w_conf.num_records,
                        CACHE_LINE,
                        CACHE_LINE,
                };
                ret = (Table**)zmalloc(sizeof(Table*));
                ret[0] = new (0) Table(ycsb_conf);
        
                /* Initialize the table */
                memset(val, 0x0, CACHE_LINE);
                for (i = 0; i < w_conf.num_records; ++i) 
                        ret[0]->Put(i, val);

        } else if (w_conf.experiment == SMALL_BANK) {
                TableConfig sb_conf = {
                        0,
                        2*w_conf.num_records,
                        start_cpu,
                        end_cpu,
                        2*w_conf.num_records,
                        CACHE_LINE,
                        CACHE_LINE,
                };
                ret = (Table**)zmalloc(2*sizeof(Table*));
                ret[0] = new (0) Table(sb_conf);
                ret[1] = new (0) Table(sb_conf);
        
                /* Initialize the table */

                for (i = 0; i < w_conf.num_records; ++i) {
                        ret[0]->Put(i, val);
                        ret[1]->Put(i, val);
                }

        } else if (w_conf.experiment == TPCC_SUBSET) {
                num_warehouses = w_conf.num_warehouses;
                num_districts = num_warehouses*NUM_DISTRICTS;
                num_customers = num_districts*NUM_CUSTOMERS;
                num_stocks = NUM_ITEMS*w_conf.num_warehouses;

                ret = (Table**)zmalloc(sizeof(Table*)*11);
                
                /* Warehouse */
                TableConfig wh_conf = {
                        0,
                        2*num_warehouses,
                        start_cpu,
                        end_cpu,
                        2*num_warehouses,
                        CACHE_LINE,
                        CACHE_LINE,
                };
                
                tbl = new(0) Table(wh_conf);
                ret[WAREHOUSE_TABLE] = tbl;
                for (i = 0; i < num_warehouses; ++i) 
                        tbl->Put(i, val);

                /* District */
                TableConfig d_conf = {
                        0,
                        2*num_districts,
                        start_cpu,
                        end_cpu,
                        2*num_districts,
                        CACHE_LINE,
                        CACHE_LINE,
                };
                
                tbl = new(0) Table(d_conf);
                for (i = 0; i < num_warehouses; ++i) {
                        for (j = 0; j < NUM_DISTRICTS; ++j) {
                                tbl->Put(tpcc_util::create_district_key(i, j),
                                         val);
                        }
                }
                ret[DISTRICT_TABLE] = tbl;
                
                /* Customer */
                TableConfig c_conf = {
                        0,
                        2*num_customers,
                        start_cpu,
                        end_cpu,
                        2*num_customers,
                        CACHE_LINE,
                        CACHE_LINE,
                };
                
                tbl = new(0) Table(c_conf);
                for (i = 0; i < num_warehouses; ++i) {
                        for (j = 0; j < NUM_DISTRICTS; ++j) {
                                for (k = 0; k < NUM_CUSTOMERS; ++k) {
                                        tbl->Put(tpcc_util::create_customer_key(i, j, k), 
                                                 val);
                                }
                        }
                }
                ret[CUSTOMER_TABLE] = tbl;

                /* Item */
                TableConfig i_conf = {
                        0,
                        2*NUM_ITEMS,
                        start_cpu,
                        end_cpu,
                        2*NUM_ITEMS,
                        CACHE_LINE,
                        CACHE_LINE,
                };
                tbl = new(0) Table(i_conf);
                for (i = 0; i < NUM_ITEMS; ++i) {
                        tbl->Put(i, val);
                }
                ret[ITEM_TABLE] = tbl;

                /* Stock */
                TableConfig s_conf = {
                        0,
                        2*num_stocks,
                        start_cpu,
                        end_cpu,
                        2*num_stocks,
                        CACHE_LINE,
                        CACHE_LINE,
                };
                tbl = new(0) Table(s_conf);
                for (i = 0; i < num_warehouses; ++i) {
                        for (j = 0; j < NUM_ITEMS; ++j) {
                                tbl->Put(tpcc_util::create_stock_key(i, j),
                                         val);
                        }
                }
                ret[STOCK_TABLE] = tbl;
        } else {
                assert(false);
        }
        return ret;
}

RecordBuffersConfig setup_buffer_config(int cpu, workload_config w_conf)
{
        RecordBuffersConfig rb_conf;
        uint32_t i;

        rb_conf.cpu = cpu;
        rb_conf.num_buffers = 500;
        if (is_ycsb_exp(w_conf)) {
                rb_conf.num_tables = 1;
                rb_conf.record_sizes = (uint32_t*)zmalloc(sizeof(uint32_t));
                rb_conf.record_sizes[0] = GLOBAL_RECORD_SIZE;
        } else if (w_conf.experiment == SMALL_BANK) {
                rb_conf.num_tables = 2;
                rb_conf.record_sizes = (uint32_t*)zmalloc(sizeof(uint32_t)*2);
                rb_conf.record_sizes[0] = GLOBAL_RECORD_SIZE;
                rb_conf.record_sizes[1] = GLOBAL_RECORD_SIZE;
        } else if (w_conf.experiment == TPCC_SUBSET) {
                rb_conf.num_tables = 11;
                rb_conf.record_sizes = (uint32_t*)zmalloc(sizeof(uint32_t)*11);
                for (i = 0; i < 11; ++i) 
                        rb_conf.record_sizes[i] = (uint32_t)tpcc_record_sizes[i];
        } else {
                assert(false);
        }
        
        return rb_conf;
}

OCCWorker** setup_occ_workers(SimpleQueue<OCCActionBatch> **inputQueue,
                              SimpleQueue<OCCActionBatch> **outputQueue,
                              table_mgr *tbls, int numThreads,
                              uint64_t epoch_threshold, uint32_t numTables, 
                              workload_config w_conf,
                              OCCConfig conf)
{
        OCCWorker **workers;
        volatile uint32_t *epoch_ptr;
        int i;
        bool is_leader;
        Table **lock_tables;

        struct OCCWorkerConfig worker_config;
        struct RecordBuffersConfig buf_config;
        workers = (OCCWorker**)malloc(sizeof(OCCWorker*)*numThreads);
        assert(workers != NULL);
        epoch_ptr = (volatile uint32_t*)alloc_mem(sizeof(uint32_t), 0);
        assert(epoch_ptr != NULL);
        barrier();
        *epoch_ptr = 0;
        barrier();
        
        /* 
         * XXX This is currently hardcoded for YCSB. Need to change it for 
         * TPC-C. 
         */
        if (READ_COMMITTED)
                lock_tables = setup_occ_lock_tables(conf, w_conf);
        else
                lock_tables = NULL;
        //        lock_tables_copy = NULL;

        /* Copy tables */
        for (i = 0; i < numThreads; ++i) {
                //                tables_copy = (Table**)alloc_mem(sizeof(Table*)*numTables, i);
                //                memcpy(tables_copy, tables, sizeof(Table*)*numTables);
                
                //                if (READ_COMMITTED) {
                //                        lock_tables_copy = (Table**)alloc_mem(sizeof(Table*), i);
                //                        memcpy(lock_tables_copy, lock_tables, sizeof(Table*));
                //                }
                //                for (i = 0; i < numTables; ++i) {
                //                        tables_copy[i] = Table::copy_table(tables[i], i);
                //                }

                is_leader = (i == 0);
                worker_config = {
                        inputQueue[i],
                        outputQueue[i],
                        i,                        
                        tbls,
                        NULL,
                        lock_tables,
                        is_leader,
                        epoch_ptr,
                        0,
                        epoch_threshold,
                        OCC_LOG_SIZE,
                        false,
                        numTables,
                };
                buf_config = setup_buffer_config(i, w_conf);
                workers[i] = new(i) OCCWorker(worker_config, buf_config);
        }
        std::cerr << "Done setting up occ workers\n";
        return workers;
}


Table* setup_single_table(uint64_t table_id, uint64_t num_buckets, 
                          int start_cpu, 
                          int end_cpu,
                          uint64_t free_list_sz,
                          uint32_t value_sz)
{
        TableConfig conf = {
                table_id,
                num_buckets,
                start_cpu,
                end_cpu,
                free_list_sz,
                value_sz,
                value_sz,
        };        
        return new(0) Table(conf);
}

/*
concurrent_table* setup_single_conc_table(uint64_t num_buckets)
{
        TableConfig conf = {
                0,
                num_buckets,
                0,
                0,
                0,
                0,
                0,
        };
        
        return new concurrent_table(conf);
}
*/

table_mgr* setup_ycsb_tables(uint64_t* num_records, workload_config w_conf, 
                             bool occ)
{
        assert(is_ycsb_exp(w_conf) == true);

        table_mgr *ret;
        Table **tables;
        
        tables = (Table**)zmalloc(sizeof(Table*));
        tables[0] = setup_single_table(0, 
                                       (uint64_t)num_records[0],
                                       0,
                                       71,
                                       2*num_records[0],
                                       occ? GLOBAL_RECORD_SIZE+8 : GLOBAL_RECORD_SIZE);
        ret = new table_mgr(tables, NULL, 1);
        return ret;
}

table_mgr* setup_small_bank_tables(uint64_t *num_records, 
                                   workload_config w_conf, 
                                   bool occ)
{
        assert(w_conf.experiment == SMALL_BANK);
        assert(num_records[0] == num_records[1]);

        table_mgr *ret;
        Table **tables;

        tables = (Table**)zmalloc(sizeof(Table*)*2);
        tables[0] = setup_single_table(0, 
                                       (uint64_t)num_records[0],
                                       0,
                                       71,
                                       2*num_records[0],
                                       occ? GLOBAL_RECORD_SIZE+8 : GLOBAL_RECORD_SIZE);
        tables[1] = setup_single_table(1, 
                                       (uint64_t)num_records[1],
                                       0,
                                       71,
                                       2*num_records[1],
                                       occ? GLOBAL_RECORD_SIZE+8 : GLOBAL_RECORD_SIZE);
        ret = new table_mgr(tables, NULL, 2);
        return ret;
}

table_mgr* setup_tpcc_tables(workload_config w_conf, bool occ)
{
        assert(w_conf.experiment == TPCC_SUBSET);
        
        table_mgr *ret;
        Table **rw_tbls;
        concurrent_table **ins_tbls;
        uint32_t i;
        
        rw_tbls = (Table**)zmalloc(sizeof(Table*)*11);
        ins_tbls = (concurrent_table**)zmalloc(sizeof(concurrent_table*)*11);

        for (i = 0; i < 11; ++i) {
                switch (i) {
                case WAREHOUSE_TABLE:
                        rw_tbls[i] = setup_single_table((uint64_t)i,
                                                        2*w_conf.num_warehouses,
                                                        0,
                                                        71,
                                                        2*w_conf.num_warehouses,
                                                        occ? sizeof(warehouse_record)+8 : sizeof(warehouse_record));
                        break;
                case DISTRICT_TABLE:
                        rw_tbls[i] = setup_single_table((uint64_t)i,
                                                        2*NUM_DISTRICTS*w_conf.num_warehouses,
                                                        0,
                                                        71,
                                                        2*NUM_DISTRICTS*w_conf.num_warehouses, 
                                                        occ? sizeof(district_record)+8:sizeof(district_record));
                        break;
                case CUSTOMER_TABLE:
                         rw_tbls[i] = setup_single_table((uint64_t)i,
                                                         2*NUM_DISTRICTS*NUM_CUSTOMERS*w_conf.num_warehouses,
                                                         0,
                                                         71,
                                                         2*NUM_DISTRICTS*NUM_CUSTOMERS*w_conf.num_warehouses,
                                                         occ? sizeof(customer_record)+8:sizeof(customer_record));
                        break;
                case NEW_ORDER_TABLE:
                        ins_tbls[i] = new concurrent_table(1000000);
                        break;
                case OORDER_TABLE:
                        ins_tbls[i] = new concurrent_table(1000000);
                        break;
                case ORDER_LINE_TABLE:
                        ins_tbls[i] = new concurrent_table(1000000);
                        break;
                case STOCK_TABLE:
                         rw_tbls[i] = setup_single_table((uint64_t)i,
                                                         2*NUM_ITEMS*w_conf.num_warehouses,
                                                         0,
                                                         71,
                                                         2*NUM_ITEMS*w_conf.num_warehouses,
                                                         occ? sizeof(stock_record)+8:sizeof(stock_record));
                        break;
                case ITEM_TABLE: 
                         rw_tbls[i] = setup_single_table((uint64_t)i,
                                                         2*NUM_ITEMS,
                                                         0,
                                                         71,
                                                         2*NUM_ITEMS,
                                                         occ? sizeof(item_record)+8:sizeof(item_record));
                        break;
                case HISTORY_TABLE:
                        ins_tbls[i] = new concurrent_table(1000000);
                        break;
                case DELIVERY_TABLE:
                         rw_tbls[i] = setup_single_table((uint64_t)i,
                                                         20*w_conf.num_warehouses,
                                                         0,
                                                         71,
                                                         20*w_conf.num_warehouses,
                                                         occ? sizeof(uint64_t)+8:sizeof(uint64_t));
                        break;
                case CUSTOMER_ORDER_INDEX:
                         rw_tbls[i] = setup_single_table((uint64_t)i,
                                                         3000*10*w_conf.num_warehouses,
                                                         0,
                                                         71,
                                                         6000*10*w_conf.num_warehouses,
                                                         occ? sizeof(uint64_t)+8:sizeof(uint64_t));
                        break;
                default:
                        assert(false);
                }
        }        
        
        ret = new table_mgr(rw_tbls, ins_tbls, 11);
        return ret;
}

table_mgr* setup_hash_tables(workload_config w_conf, bool occ)
{
        uint64_t num_records[2];
        num_records[0] = w_conf.num_records;
        num_records[1] = w_conf.num_records;

        if (is_ycsb_exp(w_conf) == true) 
                return setup_ycsb_tables(num_records, w_conf, occ);
        else if (w_conf.experiment == SMALL_BANK)
                return setup_small_bank_tables(num_records, w_conf, occ);
        else if (w_conf.experiment == TPCC_SUBSET) 
                return setup_tpcc_tables(w_conf, occ);
        else 
                assert(false);
        return NULL;
}

/*
Table** setup_hash_tables(uint32_t num_tables, uint32_t *num_records, bool occ)
{
        Table **tables;
        uint32_t i;
        TableConfig conf;

        tables = (Table**)malloc(sizeof(Table*)*num_tables);
        for (i = 0; i < num_tables; ++i) {
                conf.tableId = i;
                conf.numBuckets = (uint64_t)num_records[i];
                conf.startCpu = 0;
                conf.endCpu = 71;
                conf.freeListSz = 2*num_records[i];
                if (occ)
                        conf.valueSz = GLOBAL_RECORD_SIZE + 8;
                else
                        conf.valueSz = GLOBAL_RECORD_SIZE;
                conf.recordSize = 0;
                tables[i] = new(0) Table(conf);
        }
        return tables;
}
*/


static OCCActionBatch setup_db(workload_config conf)
{
        txn **loader_txns;
        uint32_t num_txns, i;
        OCCActionBatch ret;

        loader_txns = NULL;
        num_txns = generate_input(conf, &loader_txns);
        assert(loader_txns != NULL);
        ret.batchSize = num_txns;
        ret.batch = (OCCAction**)malloc(sizeof(mv_action*)*num_txns);
        for (i = 0; i < num_txns; ++i) 
                ret.batch[i] = setup_occ_action(loader_txns[i]);
        return ret;
}


void write_occ_output(struct occ_result result, OCCConfig config, 
                      workload_config w_conf)
{
        double elapsed_milli;
        timespec elapsed_time;
        std::ofstream result_file;
        elapsed_time = result.time_elapsed;
        elapsed_milli =
                1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
        std::cout << elapsed_milli << '\n';
        result_file.open("occ.txt", std::ios::app | std::ios::out);
        result_file << "time:" << elapsed_milli << " txns:" << result.num_txns;
        result_file << " threads:" << config.numThreads << " occ ";
        result_file << "records:" << config.numRecords << " ";
        result_file << "read_pct:" << config.read_pct << " ";
        result_file << "txn_size:" << w_conf.txn_size << " ";
        if (config.experiment == 0) 
                result_file << "10rmw" << " ";
        else if (config.experiment == 1)
                result_file << "8r2rmw" << " ";
        else if (config.experiment == 2)
                result_file << "2r8w" << " ";
        else if (config.experiment == 3) 
                result_file << "small_bank" << " "; 
        if (config.distribution == 0) 
                result_file << "uniform" << "\n";        
        else if (config.distribution == 1) 
                result_file << "zipf theta:" << config.theta << "\n";
        result_file.close();  
}

uint64_t get_completed_count(OCCWorker **workers, uint32_t num_workers,
                             OCCActionBatch *input_batches)
{
        volatile uint64_t cur;
        uint64_t total_completed = 0;
        uint32_t i;
        for (i = 0; i < num_workers; ++i) {
                cur = workers[i]->NumCompleted();
                if (cur >= (input_batches[i].batchSize)) {
                        total_completed += cur;
                } else {
                        total_completed = 0;
                        break;
                }
        }
        return total_completed;
}

uint64_t wait_to_completion(__attribute__((unused)) SimpleQueue<OCCActionBatch> **output_queues,
                            uint32_t num_workers, OCCWorker **workers)
{        
        uint32_t i;
        uint64_t num_completed = 0;
        //        OCCActionBatch temp;
        //        for (i = 1; i < num_workers; ++i) 
        //                output_queues[i]->DequeueBlocking();
        
        num_completed = 0;
        sleep(30);
        for (i = 1; i < num_workers; ++i) 
                num_completed += workers[i]->NumCompleted();
        std::cerr << num_completed << "\n";

        return num_completed;
}

void populate_tables(SimpleQueue<OCCActionBatch> *input_queue,
                     SimpleQueue<OCCActionBatch> *output_queue,
                     OCCActionBatch input,
                     table_mgr *tbls)
{
        input_queue->EnqueueBlocking(input);
        barrier();
        output_queue->DequeueBlocking();
        tbls->set_init();
}

void dry_run(SimpleQueue<OCCActionBatch> **input_queues, 
             SimpleQueue<OCCActionBatch> **output_queues,
             OCCActionBatch *input_batches,
             uint32_t num_workers)
{
        uint32_t i;
        for (i = 1; i < num_workers; ++i) 
                input_queues[i]->EnqueueBlocking(input_batches[i-1]);
        barrier();
        for (i = 1; i < num_workers; ++i) 
                output_queues[i]->DequeueBlocking();
}

void check_tables(OCCConfig config, table_mgr *tbls)
{
        uint32_t i, j, num_warehouses;
        warehouse_record *wh;
        district_record *d;
        void *record;
        uint64_t key;

        if (config.experiment != TPCC_SUBSET)
                return;
        
        num_warehouses = tpcc_config::num_warehouses;
        for (i = 0; i < num_warehouses; ++i) {
                record = tbls->get_table(WAREHOUSE_TABLE)->Get((uint64_t)i);
                wh = (warehouse_record*)&(((uint64_t*)record)[1]);
                assert(wh->w_id == i);
        }
        
        for (i = 0; i < num_warehouses; ++i) {
                for (j = 0; j < NUM_DISTRICTS; ++j) {
                        key = tpcc_util::create_district_key(i, j);
                        record = tbls->get_table(DISTRICT_TABLE)->Get(key);
                        d = (district_record*)&(((uint64_t*)record)[1]);
                        assert(d->d_w_id == i);
                        assert(d->d_id == j);
                }
        }
}

struct occ_result do_measurement(SimpleQueue<OCCActionBatch> **inputQueues,
                                 SimpleQueue<OCCActionBatch> **outputQueues,
                                 OCCWorker **workers,
                                 OCCActionBatch **inputBatches,
                                 uint32_t num_batches,
                                 OCCConfig config,
                                 OCCActionBatch setup_txns,
                                 table_mgr *tbls)
{
        timespec start_time, end_time;
        uint32_t i, j;
        struct occ_result result;
                std::cerr << "Num batches " << num_batches << "\n";
        for (i = 0; i < config.numThreads; ++i) {
                workers[i]->Run();
                workers[i]->WaitInit();
        }

        populate_tables(inputQueues[1], outputQueues[1], setup_txns, tbls);
        std::cerr << "Done populating tables\n";


        dry_run(inputQueues, outputQueues, inputBatches[0], config.numThreads);
        check_tables(config, tbls);
        std::cerr << "Done dry run\n";
        barrier();
        clock_gettime(CLOCK_REALTIME, &start_time);
        barrier();
        for (i = 0; i < num_batches; ++i) 
                for (j = 0; j < config.numThreads-1; ++j) 
                        inputQueues[j+1]->EnqueueBlocking(inputBatches[i+1][j]);
        barrier();
        result.num_txns = wait_to_completion(outputQueues, config.numThreads, 
                                             workers);
        barrier();
        clock_gettime(CLOCK_REALTIME, &end_time);
        barrier();
        result.time_elapsed = diff_time(end_time, start_time);
        for (i = 0; i < config.numThreads-1; ++i)
                result.num_txns -= inputBatches[0][i].batchSize;
        //        result.num_txns = config.numTxns;
        std::cout << "Num completed: " << result.num_txns << "\n";
        return result;
}

struct occ_result run_occ_workers(SimpleQueue<OCCActionBatch> **inputQueues,
                                  SimpleQueue<OCCActionBatch> **outputQueues,
                                  OCCWorker **workers,
                                  OCCActionBatch **inputBatches,
                                  uint32_t num_batches,
                                  OCCConfig config, OCCActionBatch setup_txns,
                                  table_mgr *tbl_mgr)
{
        int success;
        struct occ_result result;        

        success = pin_thread(79);
        assert(success == 0);
        result = do_measurement(inputQueues, outputQueues, workers,
                                inputBatches, num_batches, config, setup_txns,
                                tbl_mgr);
        std::cerr << "Done experiment!\n";
        return result;
}

void occ_experiment(OCCConfig occ_config, workload_config w_conf)
{
        SimpleQueue<OCCActionBatch> **input_queues, **output_queues;
        table_mgr *tbls;
        OCCWorker **workers;
        OCCActionBatch **inputs;
        OCCActionBatch setup_txns;
        
        struct occ_result result;

        tpcc_config::num_warehouses = w_conf.num_warehouses;
	occ_config.occ_epoch = OCC_EPOCH_SIZE;
        input_queues = setup_queues<OCCActionBatch>(occ_config.numThreads,
                                                    1024);
        output_queues = setup_queues<OCCActionBatch>(occ_config.numThreads,
                                                     1024);
        setup_txns = setup_db(w_conf);
        tbls = setup_hash_tables(w_conf, true);
        workers = setup_occ_workers(input_queues, output_queues, tbls,
                                    occ_config.numThreads, 
                                    occ_config.occ_epoch,
                                    2, 
                                    w_conf,
                                    occ_config);
        inputs = setup_occ_input(occ_config, w_conf, 1);
        pin_memory();
        result = run_occ_workers(input_queues, output_queues, workers,
                                 inputs, 1+1, occ_config,
                                 setup_txns, tbls);
        write_occ_output(result, occ_config, w_conf);
}
