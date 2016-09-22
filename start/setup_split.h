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
#include <algorithm>
#include <tpcc.h>

#define LCK_TBL_SZ	(((uint64_t)1) << 29)	/* 512 M */
#define SIMPLE_SZ 	2			/* simple action size */

extern uint32_t rand_salt;

extern uint32_t GLOBAL_RECORD_SIZE;
extern uint32_t get_partition(uint64_t record, uint32_t table, 
                              uint32_t num_partitions);
extern uint32_t get_tpcc_partition(uint32_t warehouse, uint32_t district, 
                                   uint32_t type, uint32_t num_partitions);

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
        
        static vector<uint32_t> *partitions_txns;
        static uint32_t num_split_tables;
        static uint64_t *split_table_sizes;
        static uint64_t *lock_table_sizes;

        static int gen_rand_range(int min, int max)
        {
                int range;                
                range = max - min + 1;
                return min + (rand() % range);
        }

        static void gen_rand_string(int min, int max, char *buf)
        {
                int ch_first, ch_last, length, i;
        
                ch_first = 'a';
                ch_last = 'z';
                length = gen_rand_range(min, max);
                for (i = 0; i < length; ++i) 
                        buf[i] = (char)gen_rand_range(ch_first, ch_last);
                buf[length] = '\0';
        }

        static uint32_t get_num_batches(split_config s_conf)
        {
                uint32_t ret;

                ret = s_conf.num_txns / s_conf.epoch_size;
                if (s_conf.num_txns % s_conf.epoch_size > 0)
                        ret += 1;
                return ret;
        }

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
                lock_table_sizes = (uint64_t*)zmalloc(num_split_tables*sizeof(uint64_t));
                lock_table_sizes[0] = s_conf.num_records;
        }

        static void setup_single_table(int partition, split_config s_conf, 
                                       Table ***tbl)
        {
                Table **init_tbl;
                TableConfig t_conf;
                //                assert(s_conf.experiment == YCSB_UPDATE);
                if (s_conf.experiment == YCSB_RW || 
                    s_conf.experiment == YCSB_UPDATE || 
                    s_conf.experiment == 0 || 
                    s_conf.experiment == 1 ||
                    s_conf.experiment == 2) {
                        t_conf.tableId = 0;
                        t_conf.numBuckets = (split_table_sizes[0])/s_conf.num_partitions;
                        t_conf.startCpu = partition;
                        t_conf.endCpu = partition+1;
                        t_conf.freeListSz = (split_table_sizes[0]*2)/s_conf.num_partitions;
                        // t_conf.valueSz = SPLIT_RECORD_SIZE(GLOBAL_RECORD_SIZE);
                        t_conf.valueSz = sizeof(split_record);
                        t_conf.recordSize = 0;
                        init_tbl = (Table**)zmalloc(sizeof(Table*));
                        init_tbl[0] = new (partition) Table(t_conf);
                } else {
                        assert(false);
                }
                
                tbl[partition] = init_tbl;
        }

        static void setup_stock_single(stock_record *stock)
        {
                int rand_pct, len, start_original;
                        
                stock->s_quantity = 10 + rand() % 90;
                stock->s_ytd = 0;
                stock->s_order_cnt = 0;
                stock->s_remote_cnt = 0;

                /* s_data */
                rand_pct = gen_rand_range(1, 100);
                len = gen_rand_range(26, 50);

                gen_rand_string(len, len, stock->s_data);
                if (rand_pct <= 10) {

                        // 10% of the time, i_data has the string "ORIGINAL" crammed 
                        // somewhere in the middle.
                        start_original = gen_rand_range(2, len-8);
                        stock->s_data[start_original] = 'O';
                        stock->s_data[start_original+1] = 'R';
                        stock->s_data[start_original+2] = 'I';
                        stock->s_data[start_original+3] = 'G';
                        stock->s_data[start_original+4] = 'I';
                        stock->s_data[start_original+5] = 'N';
                        stock->s_data[start_original+6] = 'A';
                        stock->s_data[start_original+7] = 'L';
                }

                gen_rand_string(24, 24, stock->s_dist_01);
                assert(strlen(stock->s_dist_01) < 25);
                gen_rand_string(24, 24, stock->s_dist_02);
                assert(strlen(stock->s_dist_02) < 25);
                gen_rand_string(24, 24, stock->s_dist_03);
                assert(strlen(stock->s_dist_03) < 25);
                gen_rand_string(24, 24, stock->s_dist_04);
                assert(strlen(stock->s_dist_04) < 25);
                gen_rand_string(24, 24, stock->s_dist_05);
                assert(strlen(stock->s_dist_05) < 25);
                gen_rand_string(24, 24, stock->s_dist_06);
                assert(strlen(stock->s_dist_06) < 25);
                gen_rand_string(24, 24, stock->s_dist_07);
                assert(strlen(stock->s_dist_07) < 25);
                gen_rand_string(24, 24, stock->s_dist_08);
                assert(strlen(stock->s_dist_08) < 25);
                gen_rand_string(24, 24, stock->s_dist_09);
                assert(strlen(stock->s_dist_09) < 25);
                gen_rand_string(24, 24, stock->s_dist_10);
                assert(strlen(stock->s_dist_10) < 25);
        }

        static void init_stock(Table *tbl, uint32_t warehouse, uint32_t item)
        {
                //                uint32_t i;
                stock_record record;

                /* Initialize records */
                //                for (i = 0; i < NUM_ITEMS; ++i) {
                record.s_w_id = warehouse;
                record.s_i_id = item;
                setup_stock_single(&record);

                tbl->Put(tpcc_util::create_stock_key(warehouse, item),
                         &record);
                        //                }                
        }
        
        static void setup_stocks(Table ***lock_tbls, Table ***data_tbls, 
                                 workload_config w_conf, 
                                 split_config s_conf, 
                                 vector<int> cpus)
        {
                uint32_t i, j, ncpus, sizes[s_conf.num_partitions], next[s_conf.num_partitions], index;
                int cpu;
                TableConfig t_conf;
                split_record splt_rec;
                stock_record *stock[s_conf.num_partitions], *temp;

                memset(sizes, 0x0, sizeof(uint32_t)*s_conf.num_partitions);
                memset(next, 0x0, sizeof(uint32_t)*s_conf.num_partitions);
                for (i = 0; i < w_conf.num_warehouses; ++i) {
                        for (j = 0; j < NUM_ITEMS; ++j) {
                                index = get_tpcc_partition(i, j, STOCK_TABLE, 
                                                           s_conf.num_partitions);
                                sizes[index] += 1;
                        }
                }

                /* Allocate tables */
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        t_conf.tableId = STOCK_TABLE;
                        t_conf.numBuckets = 2*sizes[i];
                        t_conf.startCpu = i;
                        t_conf.endCpu = i+1;
                        t_conf.freeListSz = sizes[i];
                        t_conf.valueSz = sizeof(split_record);
                        t_conf.recordSize = sizeof(split_record);
                        //                        lock_tbls[i][STOCK_TABLE] = new(i) Table(t_conf);
                        //                        stock[i] = (stock_record*)alloc_mem(sizeof(stock_record)*sizes[i], i);
                        //                        memset(stock[i], 0x0, sizeof(stock_record)*sizes[i]);

                        
                        t_conf.valueSz = sizeof(stock_record);
                        t_conf.recordSize = sizeof(stock_record);
                        t_conf.numBuckets = 2*sizes[i];
                        t_conf.freeListSz = 2*sizes[i];
                        
                        data_tbls[i][STOCK_TABLE] = new(i) Table(t_conf);
                }

                /* Insert stock records */
                for (i = 0; i < w_conf.num_warehouses; ++i) {
                        for (j = 0; j < NUM_ITEMS; ++j) {
                                cpu = get_tpcc_partition(i, j, STOCK_TABLE, s_conf.num_partitions);
                                index = next[cpu];
                                next[cpu] += 1;
                                temp = &stock[cpu][index];
                                
                                /*
                                splt_rec.key.key = tpcc_util::create_stock_key(i, j);
                                splt_rec.key.table_id = STOCK_TABLE;
                                splt_rec.epoch = 0;
                                splt_rec.key_struct = NULL;
                                splt_rec.value = (char*)temp;

                                temp->s_w_id = i;
                                temp->s_i_id = j;
                                setup_stock_single(temp);
                                */
                                //                                lock_tbls[cpu][STOCK_TABLE]->Put(tpcc_util::create_stock_key(i, j), 
                                //                                                                 &splt_rec);
                                init_stock(data_tbls[cpu][STOCK_TABLE], i, j);
                        }
                }
        }        
        
        static void setup_districts(Table ***lock_tbls, Table ***data_tbls, 
                                    workload_config w_conf, 
                                    split_config s_conf)
        {
                uint32_t i, j, cpu, index, sizes[s_conf.num_partitions], next[s_conf.num_partitions];
                district_record *d_recs[s_conf.num_partitions];
                district_record *dist_rec;
                split_record splt_rec;
                char contiguous_zip[] = "123456789";
                TableConfig t_conf;
                uint64_t key;


                memset(sizes, 0x0, sizeof(uint32_t)*s_conf.num_partitions);
                memset(next, 0x0, sizeof(uint32_t)*s_conf.num_partitions);
                for (i = 0; i < w_conf.num_warehouses; ++i) {
                        for (j = 0; j < NUM_DISTRICTS; ++j) {
                                index = get_tpcc_partition(i, j, DISTRICT_TABLE, s_conf.num_partitions);
                                sizes[index] += 1;
                        }
                }

                /* Allocate tables */
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        t_conf.tableId = DISTRICT_TABLE;
                        t_conf.numBuckets = 2*sizes[i];
                        t_conf.startCpu = i;
                        t_conf.endCpu = i+1;
                        t_conf.freeListSz = sizes[i];
                        t_conf.valueSz = sizeof(district_record);
                        t_conf.recordSize = sizeof(district_record);
                        //                        lock_tbls[i][DISTRICT_TABLE] = new(i) Table(t_conf);
                        data_tbls[i][DISTRICT_TABLE] = new(i) Table(t_conf);
                        d_recs[i] = (district_record*)alloc_mem(sizeof(district_record)*sizes[i], i);
                        assert(sizes[i] == 0 || d_recs[i] != NULL);
                        memset(d_recs[i], 0x0, sizeof(district_record)*sizes[i]);
                }


                /* Allocate tables 
                t_conf.tableId = DISTRICT_TABLE;
                t_conf.numBuckets = 2*w_conf.num_warehouses*NUM_DISTRICTS;
                t_conf.startCpu = cpu;
                t_conf.endCpu = cpu+1;
                t_conf.freeListSz = w_conf.num_warehouses*NUM_DISTRICTS;
                t_conf.valueSz = sizeof(split_record);
                t_conf.recordSize = sizeof(split_record);
                //                lock_tbls[cpu][DISTRICT_TABLE] = new(cpu) Table(t_conf);
                
                dist_rec = (district_record*)alloc_mem(sizeof(district_record)*w_conf.num_warehouses*NUM_DISTRICTS,
                                                       cpu);
                assert(dist_rec != NULL);
                memset(dist_rec, 0x0, sizeof(district_record)*w_conf.num_warehouses*NUM_DISTRICTS);


                t_conf.valueSz = sizeof(district_record);
                t_conf.recordSize = sizeof(district_record);
                data_tbls[cpu][DISTRICT_TABLE] = new(cpu) Table(t_conf);
                //                data_tbls[cpu][DISTRICT_TABLE] = NULL;
                */

                /* Initialize */
                for (i = 0; i < w_conf.num_warehouses; ++i) {
                        for (j = 0; j < NUM_DISTRICTS; ++j) {
                                cpu = get_tpcc_partition(i, j, DISTRICT_TABLE, s_conf.num_partitions);
                                index = next[cpu];
                                next[cpu] += 1;
                                dist_rec = &d_recs[cpu][index];

                                key = tpcc_util::create_district_key(i, j);
                                dist_rec->d_id = j;
                                dist_rec->d_w_id = i;
                                dist_rec->d_ytd = 3000;
                                dist_rec->d_tax = (rand() % 2001) / 1000.0;
                                dist_rec->d_next_o_id = 3000;

                                gen_rand_string(6, 10, dist_rec->d_name);
                                gen_rand_string(10, 20, dist_rec->d_street_1);
                                gen_rand_string(10, 20, dist_rec->d_street_2);
                                gen_rand_string(10, 20, dist_rec->d_city);
                                gen_rand_string(3, 3, dist_rec->d_state);

                                strcpy(dist_rec->d_zip, contiguous_zip);
                                
                                /* Insert the lock */
                                /*
                                splt_rec.key.key = key;
                                splt_rec.key.table_id = DISTRICT_TABLE;
                                splt_rec.epoch = 0;
                                splt_rec.key_struct = NULL;
                                splt_rec.value = (char*)dist_rec;
                                lock_tbls[cpu][DISTRICT_TABLE]->Put(key, &splt_rec);
                                */
                                
                                data_tbls[cpu][DISTRICT_TABLE]->Put(key, &dist_rec);
                        }
                }
                for (i = 0; i < s_conf.num_partitions; ++i)
                        assert(sizes[i] == next[i]);
        }

        static void init_c_single(customer_record *c)
        {
                uint32_t i;

                if (rand() % 100 < 10) {		// 10% Bad Credit
                        c->c_credit[0] = 'B';
                        c->c_credit[1] = 'C';
                        c->c_credit[2] = '\0';
                }
                else {		// 90% Good Credit
                        c->c_credit[0] = 'G';
                        c->c_credit[1] = 'C';
                        c->c_credit[2] = '\0';
                }                
                gen_rand_string(8, 16, c->c_first);
        
                /* XXX NEED THIS TO LOOKUP CUSTOMERS BY LAST NAME */
                //        random.gen_last_name_load(c.c_last);
                //        s_last_name_index->Put(customer.c_last, &customer);

                c->c_credit_lim = 50000;
                c->c_balance = -10;
                c->c_ytd_payment = 10;
                c->c_payment_cnt = 1;
                c->c_delivery_cnt = 0;        

                gen_rand_string(10, 20, c->c_street_1);
                gen_rand_string(10, 20, c->c_street_2);
                gen_rand_string(10, 20, c->c_city);
                gen_rand_string(3, 3, c->c_state);
                gen_rand_string(4, 4, c->c_zip);

                for (i = 4; i < 9; ++i) 
                        c->c_zip[i] = '1';            

                gen_rand_string(16, 16, c->c_phone);

                c->c_middle[0] = 'O';
                c->c_middle[1] = 'E';
                c->c_middle[2] = '\0';
                gen_rand_string(300, 500, c->c_data);
        }
        
        static void init_cust(Table *tbl, uint32_t warehouse, uint32_t district)
        {
                customer_record record;
                uint32_t cust;

                /* Initialize customers */
                for (cust = 0; cust < NUM_CUSTOMERS; ++cust) {
                        record.c_id = cust;
                        record.c_d_id = district;
                        record.c_w_id = warehouse;
                        init_c_single(&record);
                        tbl->Put(tpcc_util::create_customer_key(warehouse, district, 
                                                                cust),
                                 &record);
                }
        }
        
        static void init_single_item(item_record *item, uint32_t item_id)
        {
                int rand_pct, len, original_start;

                item->i_id = item_id;
                gen_rand_string(14, 24, item->i_name);
                item->i_price = (100 + (rand() % 9900)) / 100.0;
                rand_pct = gen_rand_range(0, 99);
                len = gen_rand_range(26, 50);

                gen_rand_string(len, len, item->i_data);
                if (rand_pct <= 10) {

                        // 10% of the time i_data has "ORIGINAL" crammed somewhere in the
                        // middle. 
                        original_start = gen_rand_range(2, len-8);
                        item->i_data[original_start] = 'O';
                        item->i_data[original_start+1] = 'R';
                        item->i_data[original_start+2] = 'I';
                        item->i_data[original_start+3] = 'G';
                        item->i_data[original_start+4] = 'I';
                        item->i_data[original_start+5] = 'N';
                        item->i_data[original_start+6] = 'A';
                        item->i_data[original_start+7] = 'L';
                }

                item->i_im_id = 1 + (rand() % 10000);
        }

        static void setup_items(Table ***tbls, split_config s_conf)
        {
                int cpu_min, cpu_max;
                TableConfig t_conf;
                Table *tbl;
                uint32_t i, ncpus;
                item_record record;

                /* Allocate table */
                t_conf.tableId = ITEM_TABLE;
                t_conf.numBuckets = 2*NUM_ITEMS;
                t_conf.startCpu = 0;
                t_conf.endCpu = s_conf.num_partitions;
                t_conf.freeListSz = NUM_ITEMS;
                t_conf.valueSz = sizeof(item_record);
                t_conf.recordSize = sizeof(item_record);                
                tbl = new(0) Table(t_conf);

                for (i = 0; i < s_conf.num_partitions; ++i) 
                        tbls[i][ITEM_TABLE] = tbl;

                /* Initialize items */
                for (i = 0; i < NUM_ITEMS; ++i) {
                        init_single_item(&record, i);
                        tbl->Put((uint64_t)i, &record);
                }
        }

        static void setup_customer_locks(Table *lock_tbl, uint32_t wh)
        {
                uint32_t i;
                split_record rec;
                uint64_t key;

                for (i = 0; i < NUM_DISTRICTS; ++i) {
                        key = tpcc_util::create_district_key(wh, i);
                        rec.key.key = key;
                        rec.key.table_id = CUSTOMER_TABLE;
                        rec.epoch = 0;
                        rec.key_struct = NULL;
                        rec.value = NULL;
                        lock_tbl->Put(key, &rec);
                }
        }

        /* 
         * Setup customer tables on various partitions. Partition round-robin by
         * warehouse 
         */
        static void setup_customers(Table ***lock_tbls, Table ***data_tbls, 
                                    workload_config w_conf, 
                                    split_config s_conf)
        {
                uint32_t i, j, index, wh, sizes[s_conf.num_partitions];
                int cpu;
                TableConfig t_conf;
                split_record splt_rec;
                uint64_t key;

                memset(sizes, 0x0, sizeof(uint32_t)*s_conf.num_partitions);
                for (i = 0; i < w_conf.num_warehouses; ++i) {
                        for (j = 0; j < NUM_DISTRICTS; ++j) {
                                index = get_tpcc_partition(i, j, CUSTOMER_TABLE,
                                                           s_conf.num_partitions);
                                sizes[index] += 1;
                        }
                }

                /* Allocate tables */
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        t_conf.tableId = CUSTOMER_TABLE;
                        t_conf.numBuckets = 2*sizes[i];
                        t_conf.startCpu = i;
                        t_conf.endCpu = i+1;
                        t_conf.freeListSz = sizes[i];
                        t_conf.valueSz = sizeof(split_record);
                        t_conf.recordSize = sizeof(split_record);
                        lock_tbls[i][CUSTOMER_TABLE] = new(i) Table(t_conf);

                        t_conf.numBuckets = 2*sizes[i]*NUM_CUSTOMERS;
                        t_conf.freeListSz = sizes[i]*NUM_CUSTOMERS;
                        t_conf.valueSz = sizeof(customer_record);
                        t_conf.recordSize = sizeof(customer_record);
                        data_tbls[i][CUSTOMER_TABLE] = new(i) Table(t_conf);
                }

                /* Initialize tables */
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        if (sizes[i] == 0) {
                                lock_tbls[i][CUSTOMER_TABLE] = NULL;
                                data_tbls[i][CUSTOMER_TABLE] = NULL;
                                continue;
                        }
                        
                        /* Setup locks */
                        t_conf.tableId = CUSTOMER_TABLE;
                        t_conf.numBuckets = 2*sizes[i];
                        t_conf.startCpu = i;
                        t_conf.endCpu = i+1;
                        t_conf.freeListSz = sizes[i];
                        t_conf.valueSz = sizeof(split_record);
                        t_conf.recordSize = sizeof(split_record);
                        lock_tbls[i][CUSTOMER_TABLE] = new(i) Table(t_conf);
                        
                        /* Setup data */
                        t_conf.numBuckets = 2*sizes[i]*NUM_CUSTOMERS;
                        t_conf.freeListSz = sizes[i]*NUM_CUSTOMERS;
                        t_conf.valueSz = sizeof(customer_record);
                        t_conf.recordSize = sizeof(customer_record);
                        data_tbls[i][CUSTOMER_TABLE] = new(i) Table(t_conf);
                }
                
                /* Insert locks and data */
                for (i = 0; i < w_conf.num_warehouses; ++i) {
                        for (j = 0; j < NUM_DISTRICTS; ++j) {
                                key = tpcc_util::create_district_key(i, j);
                                cpu = get_tpcc_partition(i, j, CUSTOMER_TABLE, 
                                                         s_conf.num_partitions);

                                /* Insert the lock */
                                splt_rec.key.key = key;
                                splt_rec.key.table_id = CUSTOMER_TABLE;
                                splt_rec.epoch = 0;
                                splt_rec.key_struct = NULL;
                                splt_rec.value = NULL;
                                lock_tbls[cpu][CUSTOMER_TABLE]->Put(key, &splt_rec);
                                init_cust(data_tbls[cpu][CUSTOMER_TABLE], i, j);
                        }
                }
        }

        static void setup_warehouses(Table ***lock_tbls, Table ***data_tbls, 
                                     workload_config w_conf, 
                                     split_config s_conf)
        {
                uint32_t i, index, cpu, sizes[s_conf.num_partitions], next[s_conf.num_partitions];
                warehouse_record *wh_recs[s_conf.num_partitions];

                //                warehouse_record *wh_rec;
                split_record splt_rec;                
                char zip[] = "123456789"; 
                warehouse_record *data;
                TableConfig t_conf;
                
                memset(sizes, 0x0, sizeof(uint32_t)*s_conf.num_partitions);
                memset(next, 0x0, sizeof(uint32_t)*s_conf.num_partitions);
                for (i = 0; i < w_conf.num_warehouses; ++i) {
                        index = get_tpcc_partition(i, 0, WAREHOUSE_TABLE, 
                                                   s_conf.num_partitions);
                        sizes[index] += 1;
                }

                /* Allocate tables */
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        t_conf.tableId = WAREHOUSE_TABLE;
                        t_conf.numBuckets = 2*sizes[i];
                        t_conf.startCpu = i;
                        t_conf.endCpu = i+1;
                        t_conf.freeListSz = sizes[i];
                        t_conf.valueSz = sizeof(warehouse_record);//sizeof(split_record);
                        t_conf.recordSize = sizeof(warehouse_record);//sizeof(split_record);
                        //                        lock_tbls[i][WAREHOUSE_TABLE] = new(i) Table(t_conf);
                        data_tbls[i][WAREHOUSE_TABLE] = new(i) Table(t_conf);
                        wh_recs[i] = (warehouse_record*)alloc_mem(sizeof(warehouse_record)*sizes[i], i);
                        assert(sizes[i] == 0 || wh_recs[i] != NULL);
                        memset(wh_recs[i], 0x0, sizeof(warehouse_record)*sizes[i]);
                }

                //                t_conf.valueSz = sizeof(warehouse_record);
                //                t_conf.recordSize = sizeof(warehouse_record);
                //                data_tbls[cpu][WAREHOUSE_TABLE] = new(cpu) Table(t_conf);
                // data_tbls[cpu][WAREHOUSE_TABLE] = NULL;

                /* Initialize */
                for (i = 0; i < w_conf.num_warehouses; ++i) {
                        cpu = get_tpcc_partition(i, 0, WAREHOUSE_TABLE, 
                                                   s_conf.num_partitions);
                        index = next[cpu];
                        next[cpu] += 1;
                        data = &wh_recs[cpu][index];

                        data->w_id = i;
                        data->w_id = i;
                        data->w_ytd = 3000;
                        data->w_tax = (rand() % 2001) / 1000.0;
                
                        gen_rand_string(6, 10, data->w_name);
                        gen_rand_string(10, 20, data->w_street_1);
                        gen_rand_string(10, 20, data->w_street_2);
                        gen_rand_string(10, 20, data->w_city);
                        gen_rand_string(3, 3, data->w_state);
                        strcpy(data->w_zip, zip);                        
                        
                        /* Insert the record */
                        data_tbls[cpu][WAREHOUSE_TABLE]->Put((uint64_t)i, &data);

                        /* Insert the lock */
                        /*
                        splt_rec.key.key = i;
                        splt_rec.key.table_id = WAREHOUSE_TABLE;
                        splt_rec.epoch = 0;
                        splt_rec.key_struct = NULL;
                        splt_rec.value = (char*)data;
                        lock_tbls[cpu][WAREHOUSE_TABLE]->Put((uint64_t)i, &splt_rec);
                        */
                }
                
                for (i = 0; i < s_conf.num_partitions; ++i)
                        assert(sizes[i] == next[i]);
        }

        static void setup_district_key_inserts(Table ***lock_tables, Table ***data_tables, 
                                               uint32_t table_id,
                                               workload_config w_conf,
                                               split_config s_conf)
        {
                assert(table_id == NEW_ORDER_TABLE || 
                       table_id == OORDER_TABLE || 
                       table_id == ORDER_LINE_TABLE ||
                       table_id == HISTORY_TABLE);
                uint32_t i, j, cpu, index, wh, d, sizes[s_conf.num_partitions];
                split_record splt_rec;
                uint64_t key;
                TableConfig t_conf;

                memset(sizes, 0x0, sizeof(uint32_t)*s_conf.num_partitions);
                for (i = 0; i < w_conf.num_warehouses; ++i) 
                        for (j = 0; j < NUM_DISTRICTS; ++j) {
                                index = get_tpcc_partition(i, j, table_id, 
                                                           s_conf.num_partitions);
                                sizes[index] += 1;
                        }
                
                /* Allocate tables */
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        if (sizes[i] == 0) {
                                lock_tables[i][table_id] = NULL;
                                data_tables[i][table_id] = NULL;
                        }

                        t_conf.tableId = table_id;
                        t_conf.numBuckets = 2*sizes[i];
                        t_conf.startCpu = i;
                        t_conf.endCpu = i+1;
                        t_conf.freeListSz = sizes[i];
                        t_conf.valueSz = sizeof(split_record);
                        t_conf.recordSize = sizeof(split_record);
                        lock_tables[i][table_id] = new(i) Table(t_conf);

                        t_conf.numBuckets = 1000000 / (s_conf.num_partitions);
                        t_conf.freeListSz = 0;
                        if (table_id == NEW_ORDER_TABLE) {
                                t_conf.valueSz = sizeof(new_order_record);
                                t_conf.recordSize = sizeof(new_order_record);
                        } else if (table_id == OORDER_TABLE) {
                                t_conf.valueSz = sizeof(oorder_record);
                                t_conf.recordSize = sizeof(oorder_record);
                        } else {
                                t_conf.valueSz = sizeof(order_line_record);
                                t_conf.recordSize = sizeof(order_line_record);
                        }
                        data_tables[i][table_id] = new(i) Table(t_conf);
                }
                
                for (i = 0; i < w_conf.num_warehouses; ++i) {
                        for (j = 0; j < NUM_DISTRICTS; ++j) {
                                key = tpcc_util::create_district_key(i, j);
                                cpu = get_tpcc_partition(i, j, table_id, 
                                                         s_conf.num_partitions);

                                /* Insert the lock */
                                splt_rec.key.key = key;
                                splt_rec.key.table_id = table_id;
                                splt_rec.epoch = 0;
                                splt_rec.key_struct = NULL;
                                splt_rec.value = NULL;
                                lock_tables[cpu][table_id]->Put(key, &splt_rec);
                        }
                }
        }

        static void setup_history(Table ***lock_tables, Table ***data_tables, 
                                  workload_config w_conf,
                                  split_config s_conf)
        {
                setup_district_key_inserts(lock_tables, data_tables, HISTORY_TABLE,
                                           w_conf,
                                           s_conf);
        }

        static void setup_new_orders(Table ***lock_tables, Table ***data_tables,
                                     workload_config w_conf,
                                     split_config s_conf)
        {

                setup_district_key_inserts(lock_tables, data_tables, NEW_ORDER_TABLE,
                                           w_conf, 
                                           s_conf);
        }

        static void setup_oorders(Table ***lock_tables, Table ***data_tables,
                                  workload_config w_conf,
                                  split_config s_conf)
        {
                setup_district_key_inserts(lock_tables, data_tables, OORDER_TABLE,
                                           w_conf, 
                                           s_conf);
        }

        static void setup_order_lines(Table ***lock_tables, Table ***data_tables,
                                      workload_config w_conf,
                                      split_config s_conf)
        {
                setup_district_key_inserts(lock_tables, data_tables, ORDER_LINE_TABLE,
                                           w_conf, 
                                           s_conf);
        }

        static void setup_tpcc_tables(Table ****lock_tables, 
                                      Table ****data_tables, 
                                      workload_config w_conf, 
                                      split_config s_conf)
        {
                uint32_t i;
                vector<int> cpus, stock_cpus, ol_cpus;
                uint32_t new_order_p;
                Table ***ltabs, ***dtabs;

                /* Each partition gets 11 tables */
                ltabs = (Table***)zmalloc(sizeof(Table**)*s_conf.num_partitions);
                dtabs = (Table***)zmalloc(sizeof(Table**)*s_conf.num_partitions);
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        ltabs[i] = (Table**)zmalloc(sizeof(Table*)*11);
                        dtabs[i] = (Table**)zmalloc(sizeof(Table*)*11);
                }
                *lock_tables = ltabs;
                *data_tables = dtabs;

                /* 
                 * Partition 0 is in charge of warehouses, districts, 
                 * and customers 
                 */
                setup_warehouses(ltabs, dtabs, w_conf, s_conf);
                
                setup_districts(ltabs, dtabs, w_conf, s_conf);
                cpus.push_back(1);
                setup_customers(ltabs, dtabs, w_conf, s_conf);
                cpus.clear();

                /* Either 0 or 1 in charge of new order and open order */
                new_order_p = 2;
                cpus.push_back((int)new_order_p);
                cpus.push_back(3);
                setup_new_orders(ltabs, dtabs, w_conf, s_conf);
                setup_history(ltabs, dtabs, w_conf, s_conf);
                setup_oorders(ltabs, dtabs, w_conf, s_conf);

                /* The rest are divided evenly among stocks and order lines */
                for (i = new_order_p; i < s_conf.num_partitions; ++i) 
                        if (i % 2 == 0)
                                stock_cpus.push_back((int)i);
                        else 
                                ol_cpus.push_back((int)i);
                setup_stocks(ltabs, dtabs, w_conf, s_conf, 
                             stock_cpus);
                setup_items(dtabs, s_conf);
                setup_order_lines(ltabs, dtabs, w_conf, s_conf);
        }

        static void setup_tables(Table ****lock_tables, Table ****data_tables, 
                                 workload_config w_conf, 
                                 split_config s_conf)
        {
                if (w_conf.experiment == YCSB_UPDATE || w_conf.experiment == YCSB_RW) {
                        *lock_tables = setup_tables(s_conf);
                        *data_tables = (Table***)zmalloc(sizeof(Table**)*s_conf.num_partitions);
                        memset(*data_tables, 0x0, sizeof(Table**)*s_conf.num_partitions);
                        init_tables(s_conf, *lock_tables);
                } else if (w_conf.experiment == TPCC_SUBSET) {
                        setup_tpcc_tables(lock_tables, data_tables, w_conf, s_conf);
                } else {
                        assert(false);
                }
        }

        static Table*** setup_tables(split_config s_conf)
        {
                uint32_t i;
                Table ***ret;

                //                assert(s_conf.experiment == YCSB_UPDATE);

                ret = (Table***)zmalloc(sizeof(Table**)*s_conf.num_partitions);
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        setup_single_table(i, s_conf, ret);                
                return ret;
        }

        /* XXX Initializes only a single table */
        static void init_tables(split_config s_conf, Table ***tbls)
        {
                uint32_t partition, index;
                uint32_t partition_sizes[s_conf.num_partitions];
                uint32_t p_indices[s_conf.num_partitions];
                char *arrays[s_conf.num_partitions], *buf;
                uint64_t i, j;
                split_record record;

                //                assert(s_conf.experiment == YCSB_UPDATE);
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        partition_sizes[i] = 0;
                
                
                for (i = 0; i < split_table_sizes[0]; ++i) {
                        partition = get_partition(i, 0, s_conf.num_partitions);
                        partition_sizes[partition] += 1;
                }

                for (i = 0; i < s_conf.num_partitions; ++i) {
                        arrays[i] = (char*)alloc_mem(partition_sizes[i]*GLOBAL_RECORD_SIZE, i);
                        memset(arrays[i], 0x0, partition_sizes[i]*GLOBAL_RECORD_SIZE);
                        p_indices[i] = 0;
                }
                
                for (i = 0; i < split_table_sizes[0]; ++i) {
                        record.key.key = (uint64_t)i;
                        record.key.table_id = 0;
                        record.epoch = 0;
                        record.key_struct = NULL;
                        partition = get_partition(i, 0, s_conf.num_partitions);
                        index = p_indices[partition];
                        p_indices[partition] += 1;
                        buf = (char*)&arrays[partition][GLOBAL_RECORD_SIZE*index];
                        record.value = buf;
                        for (j = 0; j < 1000/sizeof(uint64_t); ++j)
                                ((uint64_t*)buf)[j] = rand();
                        tbls[partition][0]->Put(i, &record);
                }
                return;
        }

        /*
        static uint32_t get_num_lock_structs()
        {
                return LCK_TBL_SZ / sizeof(lock_struct);        
        }
        */

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
                vector<int> *edge_map;

                edge_map = node->in_links;
                if (edge_map == NULL)
                        return 0;
                return edge_map->size();
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

        static void flatten_rvp(rendezvous_point *rvp)
        {
                uint32_t i, num_actions;
                split_action *rvp_ptr;

                if (rvp->flattened == true)
                        return;
                rvp->flattened = true;
                
                /* Count the number of actions */
                rvp_ptr = rvp->to_run;
                num_actions = 0;
                while (rvp_ptr != NULL) {
                        num_actions += 1;
                        rvp_ptr = rvp_ptr->get_rvp_sibling();
                }
                
                rvp->actions = (split_action**)zmalloc(sizeof(split_action*)*num_actions);
                rvp_ptr = rvp->to_run;
                for (i = 0; i < num_actions; ++i) {
                        assert(rvp_ptr != NULL);
                        rvp->actions[i] = rvp_ptr;
                        rvp_ptr = rvp_ptr->get_rvp_sibling();
                }
                assert(rvp_ptr == NULL);
        }

        static void flatten_all_rvps(txn_graph *graph)
        {
                split_action *action;
                rendezvous_point **rvps;
                uint32_t i, j, num_nodes, nrvps;
                vector<graph_node*> *nodes;

                nodes = graph->get_nodes();
                num_nodes = nodes->size();
                for (i = 0; i < num_nodes; ++i) {
                        action = (*nodes)[i]->t;
                        nrvps = action->num_downstream_rvps();
                        rvps = action->get_rvps();
                        for (j = 0; j < nrvps; ++j) {
                                flatten_rvp(rvps[j]);
                        }
                }
        }
        
        static split_action* txn_to_piece(txn *txn, uint32_t partition_id, 
                                          uint64_t dependency_flag, 
                                          bool can_commit, 
                                          bool is_post)
        {
                uint32_t num_reads, num_rmws, num_writes, max, i;
                big_key *key_array;
                split_action *action;

                action = new split_action(txn, partition_id, dependency_flag, 
                                          can_commit, is_post);
                txn->set_translator(action);
                num_reads = txn->num_reads();
                num_writes = txn->num_writes();
                num_rmws = txn->num_rmws();
                assert(is_post == false || (num_reads + num_writes + num_rmws == 0));

                if (num_reads >= num_writes && num_reads >= num_rmws) 
                        max = num_reads;
                else if (num_rmws >= num_writes && num_rmws >= num_reads)
                        max = num_rmws;
                else 
                        max = num_writes;
                key_array = (big_key*)zmalloc(sizeof(big_key)*max);
                
                txn->get_reads(key_array);
                for (i = 0; i < num_reads; ++i) 
                        action->_readset.push_back(key_array[i]);
                
                txn->get_rmws(key_array);
                for (i = 0; i < num_rmws; ++i) {
                        action->_writeset.push_back(key_array[i]);
                }

                txn->get_writes(key_array);
                for (i = 0; i < num_writes; ++i) {
                        action->_writeset.push_back(key_array[i]);
                }
                free(key_array);
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
                
                if (node->after != NULL) 
                        node->after_t = txn_to_piece(node->after, node->partition, 0, false, true);
                if (node->in_links == NULL) {
                        piece = txn_to_piece(node->app, node->partition, 0, 
                                             node->abortable, false);
                        piece->set_rvp(NULL);
                        node->t = piece;
                } else {
                        piece = txn_to_piece(node->app, node->partition, 1, 
                                             node->abortable, false);
                        if ((node_phase = find_phase(node->in_links, phases)) != NULL) {
                                rvp = node_phase->rvp;
                        } else {
                                rvp = new rendezvous_point();
                                node_phase = (txn_phase*)zmalloc(sizeof(txn_phase));
                                node_phase->parent_nodes = node->in_links;
                                node_phase->rvp = rvp;
                                phases->push_back(node_phase);
                                barrier();
                                rvp->counter = get_parent_count(node);
                                rvp->num_actions = rvp->counter;
                                rvp->done = 0;
                                barrier();
                                rvp->to_run = NULL;
                                rvp->after_txn = NULL;
                                rvp->flattened = false;
                        }
                        piece->set_rvp(rvp);
                        node->t = piece;                
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

                piece = node->t;
                find_desc_rvps(node->index, phases, &desc_rvps);
                count = desc_rvps.size();
                rvps = (rendezvous_point**)zmalloc(sizeof(rendezvous_point*)*count);
                
                /* HACK: Needed for rvp's after_txn to work correctly */
                assert(count <= 1);

                for (i = 0; i < count; ++i) {
                        assert((node->after == NULL && node->after_t == NULL) ||
                               (node->after != NULL && node->after_t != NULL));
                        assert(node->after == NULL || 
                               desc_rvps[i]->rvp->after_txn == NULL);
                        if (node->after != NULL) {
                                desc_rvps[i]->rvp->after_txn = node->after_t;
                        }
                        rvps[i] = desc_rvps[i]->rvp;
                }
                piece->set_rvp_wakeups(rvps, count);
        }
        
        static void find_abortables(txn_graph *graph) 
        {
                vector<graph_node*> *nodes;
                graph_node *cur_node;
                commit_rvp *rvp;
                split_action **actions;
                uint32_t abortable_count, i, j, sz;
                bool has_rvp = true;
                rendezvous_point *down_rvp;
                rendezvous_point **down_array;

                nodes = graph->get_nodes();
                sz = nodes->size();
                abortable_count = 0;
                for (i = 0; i < sz; ++i) {
                        cur_node = (*nodes)[i];
                        if (cur_node->abortable == true) {
                                
                                abortable_count += 1;     
                                if (cur_node->t->num_downstream_rvps() == 0)
                                        has_rvp = false;
                        }                   
                }                
                
                down_rvp = NULL;
                if (has_rvp == false) {
                        down_rvp = new rendezvous_point();
                        down_rvp->counter = abortable_count;
                        down_rvp->done = 0;
                        down_rvp->flattened = true;
                        down_rvp->num_actions = 0;
                        down_rvp->to_run = NULL;
                        down_rvp->actions = NULL;
                        down_rvp->after_txn = NULL;
                }

                actions = (split_action**)zmalloc(sizeof(split_action*)*abortable_count);
                rvp = (commit_rvp*)zmalloc(sizeof(commit_rvp));
                rvp->num_actions = abortable_count;
                rvp->to_notify = actions;
                barrier();
                rvp->num_committed = 0;
                rvp->status = (uint64_t)ACTION_UNDECIDED;
                barrier();
                
                for (i = 0, j = 0; i < sz; ++i) {
                        cur_node = (*nodes)[i];
                        if (cur_node->abortable == true) {
                                actions[j] = cur_node->t;
                                cur_node->t->set_commit_rvp(rvp);
                                if (has_rvp == false) {
                                        assert(down_rvp != NULL);
                                        down_array = (rendezvous_point**)zmalloc(sizeof(rendezvous_point*));
                                        down_array[0] = down_rvp;
                                        cur_node->t->set_rvp_wakeups(down_array, 1);
                                }
                                j += 1;
                        }
                }
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
                        actions->push_back(topo_list->t);
                        topo_list = topo_list->topo_link;
                        i += 1;
                }
                assert(i == num_nodes);                        
        }

        static void graph_to_txn(txn_graph *graph, vector<split_action*> *actions)
        {
                assert(actions != NULL && actions->size() == 0);

                uint32_t i, num_nodes;
                vector<txn_phase*> phases;
                vector<graph_node*> *nodes;
        
                nodes = graph->get_nodes();
                num_nodes = nodes->size();
        
                /* Setup rvps */
                for (i = 0; i < num_nodes; ++i) 
                        proc_downstream_node((*nodes)[i], &phases);
                for (i = 0; i < num_nodes; ++i)
                        proc_upstream_node((*nodes)[i], &phases);
                find_abortables(graph);
                flatten_all_rvps(graph);

                /* Get rid of phases */
                for (i = 0; i < phases.size(); ++i) 
                        free(phases[i]);
        
                /* Output actions, in order */
                gen_piece_array(graph, actions);
        }

        static void setup_single_action(split_config s_conf, workload_config w_conf, 
                                        vector<split_action*> **actions)
        {
                txn_graph *graph;
                vector<split_action*> generated;
                uint32_t i, sz, partition;
                split_action *cur;

                graph = generate_split_action(w_conf, s_conf.num_partitions);
                graph_to_txn(graph, &generated);
                sz = generated.size();
                for (i = 0; i < sz; ++i) {
                        cur = generated[i];
                        partition = cur->get_partition_id();
                        actions[partition]->push_back(cur);
                }
                delete(graph);
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

                temp = (vector<split_action*>**)zmalloc(s_conf.num_partitions*sizeof(vector<split_action*>*));
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        temp[i] = new vector<split_action*>();
        
                for (i = 0; i < batch_sz; ++i) 
                        setup_single_action(s_conf, w_conf, temp);

                input = vector_to_batch(s_conf.num_partitions, temp);
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        delete(temp[i]);
                free(temp);
                return input;
        }

        static split_action_batch** setup_input(split_config s_conf, workload_config w_conf)
        {
                split_action_batch **batches;
                uint32_t num_batches, i, remaining;
                
                num_batches = get_num_batches(s_conf);

                /* We're generating only two batches for now */
                batches = (split_action_batch**)zmalloc((1+num_batches)*sizeof(split_action_batch*));
                batches[0] = setup_action_batch(s_conf, w_conf, 1);

                std::cerr << num_batches << "\n";                
                remaining = s_conf.num_txns;
                for (i = 1; i < num_batches + 1; ++i) {
                        if (remaining >= s_conf.epoch_size) {
                                batches[i] = setup_action_batch(s_conf, w_conf, s_conf.epoch_size);
                                remaining -= s_conf.epoch_size;
                        } else if (s_conf.epoch_size - remaining > 0) {
                                batches[i] = setup_action_batch(s_conf, w_conf, remaining);
                                remaining -= remaining;
                        } else {
                                assert(false);
                        }
                }
                assert(remaining == 0);
                return batches;
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
        /*
        static struct lock_table_config setup_lock_table_config(uint32_t cpu, 
                                                                __attribute__((unused)) split_config s_conf)
        {

                uint32_t num_lock_structs;
                num_lock_structs = get_num_lock_structs();
                struct lock_table_config ret = {
                        cpu,
                        num_split_tables,
                        lock_table_sizes,
                        num_lock_structs,
                };
                return ret;
        }
        */

        static struct split_executor_config setup_exec_config(uint32_t cpu, 
                                                              uint32_t num_partitions, 
                                                              uint32_t partition_id,
                                                              splt_inpt_queue *input_queue,
                                                              splt_inpt_queue *output_queue,
                                                              split_config s_conf, 
                                                              Table **lock_tables,
                                                              Table **data_tables)
        {
                struct split_executor_config exec_conf;

                exec_conf = {
                        cpu,
                        num_partitions,
                        partition_id,
                        s_conf.num_outstanding,
                        input_queue,
                        output_queue,
                        lock_tables,
                        data_tables,
                };
                return exec_conf;
        }

        /*
         * Setup executor threads.
         */
        static split_executor** setup_threads(split_config s_conf, 
                                              splt_inpt_queue **in_queues,
                                              splt_inpt_queue **out_queues,
                                              Table ***lock_tables,
                                              Table ***data_tables)
                                              
        {
                split_executor **ret;
                split_executor_config conf;
                uint32_t i;

                ret = (split_executor**)
                        zmalloc(sizeof(split_executor*)*s_conf.num_partitions);
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        assert(in_queues[i] != NULL && out_queues[i] != NULL);
                        conf = setup_exec_config(i, s_conf.num_partitions, i, 
                                                 in_queues[i],
                                                 out_queues[i],
                                                 s_conf,
                                                 lock_tables[i],
                                                 data_tables[i]);
                        ret[i] = new(i) split_executor(conf);
                        ret[i]->Run();
                }
                for (i = 0; i < s_conf.num_partitions; ++i) 
                        ret[i]->WaitInit();
                return ret;
        }

        /* 
         * XXX Measurements need to be more sophisticated. For now, use a single batch 
         * to warmup, and another one to measure throughput.
         */ 
        static timespec do_experiment(split_action_batch** inputs, 
                                      splt_inpt_queue **input_queues, 
                                      splt_inpt_queue **output_queues,
                                      split_config s_conf)
        {
                uint32_t i, j, num_batches;
                split_action_batch *cur_batch;
                timespec start_time, end_time;

                /* Do warmup */
                cur_batch = inputs[0];
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        input_queues[i]->EnqueueBlocking(cur_batch[i]);
                }
                for (i = 0; i < s_conf.num_partitions; ++i) {
                        output_queues[i]->DequeueBlocking();
                }
                
                /* Do real batch */
                num_batches = get_num_batches(s_conf);
                barrier();
                clock_gettime(CLOCK_REALTIME, &start_time);
                barrier();
                for (j = 0; j < num_batches; ++j) {
                        for (i = 0; i < s_conf.num_partitions; ++i) 
                                input_queues[i]->EnqueueBlocking(inputs[j+1][i]);
                }
                
                for (j = 0; j < num_batches; ++j) {
                        for (i = 0; i < s_conf.num_partitions; ++i) 
                                output_queues[i]->DequeueBlocking();
                }
                
                barrier();
                clock_gettime(CLOCK_REALTIME, &end_time);
                barrier();
                
                return diff_time(end_time, start_time);
        }

        static void write_output(split_config conf,
                                 double elapsed_milli, 
                                 workload_config w_conf)
        {
                std::ofstream result_file;

                result_file.open("split.txt", std::ios::app | std::ios::out);
                result_file << "split ";
                result_file << "time:" << elapsed_milli << " ";
                result_file << "txns:" << conf.num_txns << " ";
                result_file << "threads:" << conf.num_partitions << " ";
                result_file << "records:" << conf.num_records << " ";
                result_file << "read_pct:" << conf.read_pct << " ";
                result_file << "txn_size:" << w_conf.txn_size << " ";
                if (conf.experiment == 0) 
                        result_file << "test " << " ";
                else if (conf.experiment == 1) 
                        result_file << "rvp test" << " ";
                else if (conf.experiment == 2) 
                        result_file << "abort test " << " ";
                else if (conf.experiment == 3) 
                        result_file << "small_bank" << " ";
                else if (conf.experiment == 4) 
                        result_file << "ycsb_update" << " " << "abort_pos:" << w_conf.abort_pos << " ";
                else if (conf.experiment == YCSB_RW)
                        result_file << "ycsb_rw" << " ";
                //                        assert(false);
 
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

        static void write_sizes() 
        {
                assert(partitions_txns != NULL);
                std::ofstream result_file;
                uint32_t i;
                double diff, cur;
                
                std::sort(partitions_txns->begin(), partitions_txns->end());
                result_file.open("txn_sizes.txt", std::ios::app | std::ios::out);
                
                diff = 1.0 / (double)(partitions_txns->size());
                cur = 0.0;
                for (i = 0; i < partitions_txns->size(); ++i) {
                        result_file << (*partitions_txns)[i] << " " << cur << "\n";
                        cur += diff;
                }
                
                result_file.close();
        }
        
        static void split_experiment(split_config s_conf, workload_config w_conf)
        {
                num_split_tables = 0;
                split_table_sizes = NULL;
                rand_salt = (uint32_t)rand();

                splt_inpt_queue **input_queues, **output_queues;
                split_action_batch **inputs;
                timespec exp_time;
                double elapsed_milli;
                Table ***lock_tables, ***data_tables;

                setup_table_info(s_conf);
                setup_tables(&lock_tables, &data_tables, w_conf, s_conf);
                assert(lock_tables != NULL && data_tables != NULL);

                input_queues = setup_input_queues(s_conf);
                output_queues = setup_input_queues(s_conf);
                
                std::cerr << "Setup queues\n";

                inputs = setup_input(s_conf, w_conf);
                
                std::cerr << "Setup input\n";
                setup_threads(s_conf, input_queues, output_queues, lock_tables, 
                              data_tables);
                std::cerr << "Setup database threads\n";
                exp_time = do_experiment(inputs, input_queues, output_queues, s_conf);
                std::cerr << "Done experiment\n";
                elapsed_milli =
                        1000.0*exp_time.tv_sec + exp_time.tv_nsec/1000000.0;
                write_output(s_conf, elapsed_milli, w_conf);
        }
};

#endif 		// SETUP_SPLIT_H_
