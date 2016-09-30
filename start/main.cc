#include <database.h>
#include <cpuinfo.h>
#include <config.h>
#include <eager_worker.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <gperftools/profiler.h>
#include <small_bank.h>
#include <setup_occ.h>
#include <setup_mv.h>
#include <setup_hek.h>
#include <setup_locking.h>
#include <setup_pipelining.h>
#include <algorithm>
#include <fstream>
#include <set>
#include <iostream>
#include <common.h>
#include <sys/mman.h>
#include <setup_split.h>
#include <tpcc.h>

#define RECYCLE_QUEUE_SIZE 64
#define INPUT_SIZE 1024
#define OFFSET 0
#define OFFSET_CORE(x) (x+OFFSET)

Database DB(2);

uint64_t dbSize = ((uint64_t)1<<36);
size_t *tpcc_record_sizes;
bool split_flag;
uint32_t GLOBAL_RECORD_SIZE;
uint32_t NUM_CC_THREADS;
uint64_t recordSize;
int NumProcs;
uint32_t numLockingRecords;
uint32_t tpcc_config::num_warehouses = 0;

uint32_t setup_split::num_split_tables = 0;
uint64_t* setup_split::split_table_sizes = NULL;
vector<uint32_t> *setup_split::partitions_txns = NULL;
uint64_t *setup_split::lock_table_sizes = NULL;

void** tpcc_config::warehouses = NULL;
void*** tpcc_config::districts = NULL; 
uint32_t* tpcc_config::txn_sizes = NULL;

uint32_t cc_type;

extern size_t convert_record_sz(size_t value_sz, ConcurrencyControl cc_type);


void init_tpcc_local(ConcurrencyControl cc_type, workload_config w_conf)
{
        assert(tpcc_config::warehouses == NULL && tpcc_config::districts == NULL);
        uint32_t i, j;
        size_t record_sz;
        void *wh, **d;

        if (w_conf.experiment != TPCC_SUBSET || 
            ((cc_type != OCC) && (cc_type != LOCKING) && (cc_type != PIPELINED)))
                return;

        tpcc_config::warehouses = (void**)zmalloc(sizeof(void*)*w_conf.num_warehouses);
        tpcc_config::districts = (void***)zmalloc(sizeof(void**)*w_conf.num_warehouses);

        for (i = 0; i < w_conf.num_warehouses; ++i) {
                /* Warehouse init */
                record_sz = sizeof(warehouse_record);
                record_sz = convert_record_sz(record_sz, cc_type);
                wh = alloc_mem(record_sz, i % 40);
                memset(wh, 0x0, record_sz);
                tpcc_config::warehouses[i] = wh;

                /* District init */
                d = (void**)alloc_mem(sizeof(void*)*NUM_DISTRICTS, i % 40);
                memset(d, 0x0, sizeof(void*)*NUM_DISTRICTS);
                for (j = 0; j < NUM_DISTRICTS; ++j) {
                        record_sz = sizeof(district_record);
                        record_sz = convert_record_sz(record_sz, cc_type);
                        d[j] = alloc_mem(record_sz, i % 40);
                        memset(d[j], 0x0, record_sz);
                }
                tpcc_config::districts[i] = d;
        }
        
        tpcc_config::txn_sizes = (uint32_t*)zmalloc(sizeof(uint32_t)*2);
        tpcc_config::txn_sizes[0] = 6;
        tpcc_config::txn_sizes[1] = 4;
}

int main(int argc, char **argv) {
        //        mlockall(MCL_FUTURE);
  srand(time(NULL));
  ExperimentConfig cfg(argc, argv);
  std::cout << cfg.ccType << "\n";


  /* Only initialize values for tables that we're actually going to use */
  tpcc_record_sizes = (size_t*)zmalloc(sizeof(size_t)*11);

  size_t convert_record_sz(size_t value_sz, ConcurrencyControl cc_type);

  tpcc_record_sizes[WAREHOUSE_TABLE] = convert_record_sz(sizeof(warehouse_record), cfg.ccType);
  tpcc_record_sizes[DISTRICT_TABLE] = convert_record_sz(sizeof(district_record), cfg.ccType);
  tpcc_record_sizes[CUSTOMER_TABLE] = convert_record_sz(sizeof(customer_record), cfg.ccType);
  tpcc_record_sizes[NEW_ORDER_TABLE] = convert_record_sz(sizeof(new_order_record), cfg.ccType);
  tpcc_record_sizes[OORDER_TABLE] = convert_record_sz(sizeof(oorder_record), cfg.ccType);
  tpcc_record_sizes[ORDER_LINE_TABLE] = convert_record_sz(sizeof(order_line_record), cfg.ccType);
  tpcc_record_sizes[STOCK_TABLE] = convert_record_sz(sizeof(stock_record), cfg.ccType);
  tpcc_record_sizes[ITEM_TABLE] = convert_record_sz(sizeof(item_record), cfg.ccType);
  tpcc_record_sizes[HISTORY_TABLE] = convert_record_sz(sizeof(history_record), cfg.ccType);
  tpcc_record_sizes[DELIVERY_TABLE] = convert_record_sz(sizeof(uint64_t), cfg.ccType);
  tpcc_record_sizes[CUSTOMER_ORDER_INDEX] = convert_record_sz(sizeof(uint64_t), cfg.ccType);

  init_tpcc_local(cfg.ccType, cfg.get_workload_config());
  cc_type = cfg.ccType;
  
  if (cfg.ccType == MULTIVERSION) {
          split_flag = false;
          if (cfg.mvConfig.experiment < 3) 
                  recordSize = cfg.mvConfig.recordSize;
          else if (cfg.mvConfig.experiment < 5)
                  recordSize = sizeof(SmallBankRecord);
          else
                  assert(false);
          if (cfg.mvConfig.experiment < 3)
                  GLOBAL_RECORD_SIZE = 1000;
          else
                  GLOBAL_RECORD_SIZE = sizeof(SmallBankRecord);

          do_mv_experiment(cfg.mvConfig, cfg.get_workload_config());
          exit(0);
  } else if (cfg.ccType == LOCKING) {
          
          split_flag = false;
          recordSize = cfg.lockConfig.record_size;
          assert(recordSize == 8 || recordSize == 1000);
          assert(cfg.lockConfig.distribution < 2);
          if (cfg.lockConfig.experiment != 3)
                  GLOBAL_RECORD_SIZE = 1000;
          else
                  GLOBAL_RECORD_SIZE = sizeof(SmallBankRecord);
          locking_experiment(cfg.lockConfig, cfg.get_workload_config());
          exit(0);
  } else if (cfg.ccType == OCC) {
          split_flag = false;
          recordSize = cfg.occConfig.recordSize;
          assert(cfg.occConfig.distribution < 2);
          assert(recordSize == 8 || recordSize == 1000);
          if (cfg.occConfig.experiment != 3)
                  GLOBAL_RECORD_SIZE = 1000;
          else
                  GLOBAL_RECORD_SIZE = sizeof(SmallBankRecord);

          occ_experiment(cfg.occConfig, cfg.get_workload_config());
          
          exit(0);
  } else if (cfg.ccType == HEK) {
          split_flag = false;
          recordSize = cfg.hek_conf.record_size;
          assert(cfg.hek_conf.distribution < 2);
          assert(recordSize == 8 || recordSize == 1000);
          do_hekaton_experiment(cfg.hek_conf);
          exit(0);
  } else if (cfg.ccType == SPLIT) {
          split_flag = true;
          recordSize = cfg.split_conf.record_size;
          assert(cfg.split_conf.distribution < 2);
          assert(recordSize == 8 || recordSize == 1000);
          GLOBAL_RECORD_SIZE = recordSize;
          setup_split::split_experiment(cfg.split_conf, cfg.get_workload_config());
          exit(0);
  } else if (cfg.ccType == PIPELINED) {

          split_flag = false;
          recordSize = cfg.lockConfig.record_size;
          assert(recordSize == 8 || recordSize == 1000);
          assert(cfg.lockConfig.distribution < 2);
          if (cfg.lockConfig.experiment != 3)
                  GLOBAL_RECORD_SIZE = 1000;
          else
                  GLOBAL_RECORD_SIZE = sizeof(SmallBankRecord);
          pipelining_experiment(cfg.lockConfig, cfg.get_workload_config());
          exit(0);
  }
}
