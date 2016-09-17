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

lck_warehouse** tpcc_config::warehouses = NULL;
lck_district** tpcc_config::districts = NULL; 
uint32_t* tpcc_config::txn_sizes = NULL;

int main(int argc, char **argv) {
        //        mlockall(MCL_FUTURE);
  srand(time(NULL));
  ExperimentConfig cfg(argc, argv);
  std::cout << cfg.ccType << "\n";

  /* Only initialize values for tables that we're actually going to use */
  tpcc_record_sizes = (size_t*)zmalloc(sizeof(size_t)*11);
  tpcc_record_sizes[WAREHOUSE_TABLE] = sizeof(warehouse_record);
  tpcc_record_sizes[DISTRICT_TABLE] = sizeof(district_record);
  tpcc_record_sizes[CUSTOMER_TABLE] = sizeof(customer_record);
  tpcc_record_sizes[NEW_ORDER_TABLE] = sizeof(new_order_record);
  tpcc_record_sizes[OORDER_TABLE] = sizeof(oorder_record);
  tpcc_record_sizes[ORDER_LINE_TABLE] = sizeof(order_line_record);
  tpcc_record_sizes[STOCK_TABLE] = sizeof(stock_record);
  tpcc_record_sizes[ITEM_TABLE] = sizeof(item_record);
  tpcc_record_sizes[HISTORY_TABLE] = sizeof(history_record);
  tpcc_record_sizes[DELIVERY_TABLE] = sizeof(uint64_t);
  tpcc_record_sizes[CUSTOMER_ORDER_INDEX] = sizeof(uint64_t);  
  
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
  }
}
