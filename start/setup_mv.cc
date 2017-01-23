#include <config.h>
#include <common.h>
#include <mv_action.h>
#include <concurrent_queue.h>
#include <preprocessor.h>
#include <executor.h>
#include <iostream>
#include <fstream>
#include <setup_workload.h>
#include <hybrid_worker.h>

#define INPUT_SIZE 256
#define OFFSET 0
#define OFFSET_CORE(x) (x+OFFSET)

#define MV_DRY_RUNS 5

static uint64_t dbSize = ((uint64_t)1<<36);
extern uint32_t GLOBAL_RECORD_SIZE;

Table** mv_tables;
uint64_t preproc_stats[160];

static void CreateQueues(int cpuNumber, uint32_t subCount, 
                         SimpleQueue<ActionBatch>*** OUT_PUB_QUEUES,
                         SimpleQueue<ActionBatch>*** OUT_SUB_QUEUES) {
        if (subCount == 0) {
                *OUT_PUB_QUEUES = NULL;
                *OUT_SUB_QUEUES = NULL;
                return;
        }
        // Allocate space to keep queues for subordinates
        void *pubTemp = alloc_mem(sizeof(SimpleQueue<ActionBatch>*)*subCount, 
                                  cpuNumber);
        void *subTemp = alloc_mem(sizeof(SimpleQueue<ActionBatch>*)*subCount,
                                  cpuNumber);
        auto pubQueues = (SimpleQueue<ActionBatch>**)pubTemp;
        auto subQueues = (SimpleQueue<ActionBatch>**)subTemp;

        // Allocate space for queue data
        char *pubArray = (char*)alloc_mem(CACHE_LINE*4*subCount, cpuNumber);
        assert(pubArray != NULL);
        memset(pubArray, 0x00, CACHE_LINE*4*subCount);
        char *subArray = &pubArray[CACHE_LINE*2*subCount];
      
        // Allocate space for queue meta-data
        auto pubMetaData = 
                (SimpleQueue<ActionBatch>*)alloc_mem(sizeof(SimpleQueue<ActionBatch>)*2*subCount, 
                                                     cpuNumber);
        assert(pubMetaData != NULL);
        memset(pubMetaData, 0x0, sizeof(SimpleQueue<ActionBatch>)*2*subCount);
        auto subMetaData = &pubMetaData[subCount];

        // Initialize meta-data with SimpleQueue constructor
        for (uint32_t i = 0; i < subCount; ++i) {
                auto pubQueue = 
                        new (&pubMetaData[i]) 
                        SimpleQueue<ActionBatch>(&pubArray[2*CACHE_LINE*i], 2);
                auto subQueue =
                        new (&subMetaData[i])
                        SimpleQueue<ActionBatch>(&subArray[2*CACHE_LINE*i], 2);
                assert(pubQueue != NULL && subQueue != NULL);
                pubQueues[i] = pubQueue;
                subQueues[i] = subQueue;
        }
  
        *OUT_PUB_QUEUES = pubQueues;
        *OUT_SUB_QUEUES = subQueues;
}

static hybrid_worker** setup_threads(MVConfig mv_config, 
                                     MVSchedulerConfig *sched_configs,
                                     ExecutorConfig *exec_configs)
{
    uint32_t num_threads, i;
    hybrid_worker **thread_ptrs;

    num_threads = (uint32_t)(mv_config.numCCThreads + mv_config.numWorkerThreads);
    thread_ptrs = (hybrid_worker**)malloc(sizeof(hybrid_worker*)*num_threads);
    
    for (i = 0; i < num_threads; ++i) 
        thread_ptrs[i] = new(i) hybrid_worker(i, sched_configs[i], exec_configs[i]);
    
    return thread_ptrs;
}

static MVSchedulerConfig SetupSched(int cpuNumber, 
                                    uint32_t threadId, 
                                    int numThreads, 
                                    size_t alloc, 
                                    uint32_t numTables,
                                    size_t *partSizes, 
                                    uint32_t numRecycles,
                                    SimpleQueue<ActionBatch> *inputQueue,
                                    uint32_t numOutputs,
                                    SimpleQueue<ActionBatch> *outputQueues)
{
        assert(inputQueue != NULL && outputQueues != NULL);
        uint32_t subCount;
        SimpleQueue<ActionBatch> **pubQueues, **subQueues;
	
	if (cpuNumber == 0) {
		subCount = numThreads - 1;
		CreateQueues(cpuNumber, subCount, &pubQueues, &subQueues);
	} else {
		subCount = 0;
		pubQueues = NULL;
                subQueues = NULL;
	}

	/* 
        if (cpuNumber % 10 == 0) {
                if (cpuNumber == 0) {
      
                        // Figure out how many subordinates this thread is in charge of.
                        uint32_t localSubordinates = numThreads > 10? 9 : numThreads-1;
                        uint32_t numRemoteSockets = 
                                numThreads/10 + (numThreads % 10 == 0? 0 : 1) - 1;
                        subCount = (uint32_t)(localSubordinates + numRemoteSockets);
                        CreateQueues(cpuNumber, subCount, &pubQueues, &subQueues);
                }
                else {
                        int myDiv = cpuNumber/10;
                        int totalDiv = numThreads/10;
                        if (myDiv < totalDiv) {
                                subCount = 9;
                        }
                        else {
                                subCount = (uint32_t)(numThreads - cpuNumber - 1);
                        }      

                        CreateQueues(cpuNumber, subCount, &pubQueues, &subQueues);      
                }
        }
        else {
                subCount = 0;
                pubQueues = NULL;
                subQueues = NULL;
        }
	*/

        // Create recycle queue
        uint32_t recycleQueueSize = CACHE_LINE*64*numRecycles;
        uint32_t queueArraySize = numRecycles*sizeof(SimpleQueue<MVRecordList>*);
        uint32_t queueMetadataSize = numRecycles*sizeof(SimpleQueue<MVRecordList>);
        uint32_t blobSize = recycleQueueSize + queueArraySize + queueMetadataSize;

        // Allocate a blob of memory for all recycle queue related data
        void *blob = alloc_mem(blobSize, cpuNumber);
        SimpleQueue<MVRecordList> **queueArray = (SimpleQueue<MVRecordList>**)blob;
        SimpleQueue<MVRecordList> *queueMetadata = 
                (SimpleQueue<MVRecordList>*)((char*)blob + queueArraySize);
        char *queueData = (char*)blob + queueArraySize + queueMetadataSize;  

        for (uint32_t i = 0; i < numRecycles; ++i) {
                uint32_t offset = i*CACHE_LINE*RECYCLE_QUEUE_SIZE;
                queueArray[i] = 
                        new (&queueMetadata[i]) 
                        SimpleQueue<MVRecordList>(queueData+offset, RECYCLE_QUEUE_SIZE);
        }

        MVSchedulerConfig cfg = {
                cpuNumber,
                threadId,
                alloc,
                numTables,
                partSizes,
                numOutputs,
                subCount,
                numRecycles,
                inputQueue,
                outputQueues,
                pubQueues,
                subQueues,
                queueArray,
        };
        return cfg;
}

static GarbageBinConfig SetupGCConfig(uint32_t numCCThreads,
                                      uint32_t numWorkerThreads,
                                      uint32_t numTables,
                                      int cpuNumber,
                                      volatile uint32_t *GClowWaterMarkPtr) {
        assert(GClowWaterMarkPtr != NULL);

        // First initialize garbage collection meta data. We need space to keep 
        // references to remote threads's GC queues.
        uint32_t concControlGC_sz = numCCThreads*sizeof(SimpleQueue<MVRecordList>*);
        uint32_t workerGC_sz = 
                numWorkerThreads*numTables*sizeof(SimpleQueue<RecordList>*);
        uint32_t gc_sz = concControlGC_sz + workerGC_sz;
        void *blob = alloc_mem(gc_sz, cpuNumber);
        memset(blob, 0x0, gc_sz);
  
        SimpleQueue<MVRecordList> **ccGCQueues = (SimpleQueue<MVRecordList>**)blob;
        SimpleQueue<RecordList> **workerGCQueues = 
                (SimpleQueue<RecordList>**)((char*)blob + concControlGC_sz);

        GarbageBinConfig gcConfig = {
                numCCThreads,
                numWorkerThreads,
                numTables,
                cpuNumber,
                GClowWaterMarkPtr,
                ccGCQueues,
                workerGCQueues,
        };
        return gcConfig;
}

// Setup GC queues for a single worker. Other worker threads will insert 
// recycled data into these queues.
static SimpleQueue<RecordList>* SetupGCQueues(uint32_t cpuNumber, 
                                              uint32_t queuesPerTable, 
                                              uint32_t numTables) {
        // Allocate a blob of data to hold queue meta data (head & tail ptrs), and
        // actual queue entries.
        uint32_t metaDataSz = 
                sizeof(SimpleQueue<RecordList>)*queuesPerTable*numTables;
        uint32_t dataSz = CACHE_LINE*RECYCLE_QUEUE_SIZE*numTables*queuesPerTable;
        uint32_t totalSz = metaDataSz + dataSz;
        void *blob = alloc_mem(totalSz, cpuNumber);
        memset(blob, 0x00, totalSz);

        // The first part of the blob corresponds to queue space, the second is queue 
        // entry space
        SimpleQueue<RecordList> *queueData = (SimpleQueue<RecordList>*)blob;
        char *data = (char*)blob + metaDataSz;

        // Use for computing appropriate offsets
        //        uint32_t qSz = sizeof(SimpleQueue<RecordList>);
        uint32_t singleDataSz = CACHE_LINE*RECYCLE_QUEUE_SIZE;
  
        // Initialize queues
        for (uint32_t i = 0; i < numTables; ++i) {
                for (uint32_t j = 0; j < queuesPerTable; ++j) {
      
                        uint32_t queueOffset = (i*queuesPerTable + j);
                        uint32_t dataOffset = queueOffset*singleDataSz;
                        new (&queueData[queueOffset]) 
                                SimpleQueue<RecordList>(data + dataOffset, RECYCLE_QUEUE_SIZE);        
                }
        }
        return queueData;
}

static ExecutorConfig SetupExec(uint32_t cpuNumber, uint32_t threadId, 
                                uint32_t numWorkerThreads, 
                                volatile uint32_t *epoch, 
                                volatile uint32_t *GClowWaterMarkPtr,
                                uint64_t *recordSizes, 
                                uint64_t *allocSizes,
                                SimpleQueue<ActionBatch> *inputQueue, 
                                SimpleQueue<ActionBatch> *outputQueue,
                                uint32_t numCCThreads,
                                uint32_t numTables, 
                                uint32_t queuesPerTable) {  
  assert(inputQueue != NULL);  
  
  // GC config
  GarbageBinConfig gcConfig = SetupGCConfig(numCCThreads, numWorkerThreads, 
                                            numTables, 
                                            cpuNumber,
                                            GClowWaterMarkPtr);

  // GC queues for this particular worker
  SimpleQueue<RecordList> *gcQueues = SetupGCQueues(cpuNumber, queuesPerTable, 
                                                    numTables);
  ExecutorConfig config = {
    threadId,
    numWorkerThreads,
    (int)cpuNumber,
    epoch,
    GClowWaterMarkPtr,
    inputQueue,
    outputQueue,
    1,
    recordSizes,
    allocSizes,
    1,
    gcQueues,
    gcConfig,
  };
  return config;
}

static ExecutorConfig* SetupExecutors(uint32_t numWorkers, 
                                      uint32_t queuesPerTable,
                                      SimpleQueue<ActionBatch> *inputQueue,
                                      SimpleQueue<ActionBatch> *outputQueue,
                                      uint32_t queuesPerCCThread,
                                      SimpleQueue<MVRecordList> ***ccQueues) 
{ 
    assert(queuesPerCCThread == numWorkers);
    assert(queuesPerTable == numWorkers);

    uint64_t threadDbSz = dbSize / numWorkers;
    ExecutorConfig *configs = (ExecutorConfig*)malloc(sizeof(ExecutorConfig)*numWorkers);
    volatile uint32_t *epochArray = 
        (volatile uint32_t*)malloc(sizeof(uint32_t)*(numWorkers+1));  
    memset((void*)epochArray, 0x0, sizeof(uint32_t)*(numWorkers+1));

    uint32_t numTables = 1;

    uint64_t *sizeData = (uint64_t*)malloc(sizeof(uint32_t)*2);
    sizeData[0] = GLOBAL_RECORD_SIZE;
    sizeData[1] = threadDbSz/recordSize;

    // First pass, create configs. Each config contains a reference to each 
    // worker's local GC queue.
    for (uint32_t i = 0; i < numWorkers; ++i) {
        SimpleQueue<ActionBatch> *curOutput = &outputQueue[i];
        //    if (i == 0) {
        //            curOutput = &outputQueue[i];
        //    }
        configs[i] = SetupExec(i, i, numWorkers, &epochArray[i], 
                               &epochArray[numWorkers],
                               &sizeData[0],
                               &sizeData[1],
                               &inputQueue[i],
                               curOutput,
                               numWorkers,
                               1,
                               queuesPerTable);
    }
  
    // Second pass, connect recycled data producers with consumers
    for (uint32_t i = 0; i < numWorkers; ++i) {
    
        // Connect to cc threads
        for (uint32_t j = 0; j < numWorkers; ++j) {
            configs[i].garbageConfig.ccChannels[j] = ccQueues[j][i%queuesPerCCThread];
            assert(configs[i].garbageConfig.ccChannels[j] != NULL);
        }

        // Connect to every workers gc queue
        for (uint32_t j = 0; j < numWorkers; ++j) {
            for (uint32_t k = 0; k < numTables; ++k) {
                configs[i].garbageConfig.workerChannels[j] = 
                    &configs[j].recycleQueues[k*queuesPerTable+(i%queuesPerTable)];
            }
            assert(configs[i].garbageConfig.workerChannels[j] != NULL);
        }
    }

    return configs;
}

// Setup an array of queues.
template<class T>
static SimpleQueue<T>* SetupQueuesMany(uint32_t numEntries, uint32_t numQueues, int cpu) {
  size_t metaDataSz = sizeof(SimpleQueue<T>)*numQueues; // SimpleQueue structs
  size_t queueDataSz = CACHE_LINE*numEntries*numQueues; // queue data
  size_t totalSz = metaDataSz+queueDataSz;
  //SimpleQueue<T> *queue_array = alloc_mem(sizeof(SimpleQueue<T>)*numQueues);
  void *data = alloc_mem(totalSz, cpu);
  memset(data, 0x00, totalSz);

  char *queueData = (char*)data + metaDataSz;
  size_t dataDelta = CACHE_LINE*numEntries;
  SimpleQueue<T> *queues = (SimpleQueue<T>*)data;

  // Initialize queue structs
  for (uint32_t i = 0; i < numQueues; ++i) {
      new (&queues[i]) SimpleQueue<T>(&queueData[dataDelta*i], numEntries);
  }
  return queues;
}

static MVSchedulerConfig* SetupSchedulers(int numProcs, 
                                          SimpleQueue<ActionBatch> **inputQueueRef_OUT, 
                                          SimpleQueue<ActionBatch> **outputQueueRefs_OUT, 
                                          uint32_t numOutputs,
                                          size_t allocatorSize, 
                                          uint32_t numTables,
                                          size_t tableSize, 
                                          SimpleQueue<MVRecordList> ***gcRefs_OUT)
{  
    size_t partitionChunk = tableSize/numProcs;
    size_t *tblPartitionSizes = (size_t*)malloc(numTables*sizeof(size_t));
    for (uint32_t i = 0; i < numTables; ++i) {
        tblPartitionSizes[i] = partitionChunk;
    }

    std::cout << "Num scheduler threads: " << numProcs << "\n";

    // Set up queues for leader thread
    char *inputArray = (char*)alloc_mem(CACHE_LINE*INPUT_SIZE, 0);            
    SimpleQueue<ActionBatch> *leaderInputQueue = 
        new SimpleQueue<ActionBatch>(inputArray, INPUT_SIZE);

    SimpleQueue<ActionBatch> *leaderOutputQueues = 
        SetupQueuesMany<ActionBatch>(INPUT_SIZE, (uint32_t)numOutputs, 0);
    
    MVSchedulerConfig *schedArray = (MVSchedulerConfig*)malloc(sizeof(MVSchedulerConfig)*numProcs);
  
    MVSchedulerConfig globalLeaderConfig = SetupSched(0, 0, numProcs, 
                                                      allocatorSize,
                                                      numTables,
                                                      tblPartitionSizes, 
                                                      numOutputs,
                                                      leaderInputQueue,
                                                      numOutputs,
                                                      leaderOutputQueues);

    schedArray[0] = globalLeaderConfig;
    gcRefs_OUT[0] = globalLeaderConfig.recycleQueues;

    for (uint32_t i = 1; i < numProcs; ++i) {
	auto inputQueue = globalLeaderConfig.pubQueues[i-1];
	auto outputQueue = globalLeaderConfig.subQueues[i-1];
	MVSchedulerConfig config = SetupSched(i, i, numProcs, allocatorSize, 
						 numTables,
						 tblPartitionSizes,
						 numOutputs, 
						 inputQueue,
	  					 1,
						 outputQueue);
	schedArray[i] = config;
	gcRefs_OUT[i] = config.recycleQueues;
    }
    /*
    for (uint32_t i = 1; i < numProcs; ++i) {
        if (i % 10 == 0) {
            int leaderNum = i/10;
            auto inputQueue = globalLeaderConfig.pubQueues[9+leaderNum-1];
            auto outputQueue = globalLeaderConfig.subQueues[9+leaderNum-1];
            MVSchedulerConfig config = SetupSched(i, i, numProcs, allocatorSize, 
                                                  numTables,
                                                  tblPartitionSizes, 
                                                  numOutputs,
                                                  inputQueue, 
                                                  1,
                                                  outputQueue);
            schedArray[i] = config; 
            gcRefs_OUT[i] = config.recycleQueues;
            localLeaderConfig = config;
        }
        else {
            int index = i%10;      
            auto inputQueue = localLeaderConfig.pubQueues[index-1];
            auto outputQueue = localLeaderConfig.subQueues[index-1];
            MVSchedulerConfig subConfig = SetupSched(i, i, numProcs, allocatorSize, 
                                                     numTables,
                                                     tblPartitionSizes, 
                                                     numOutputs,
                                                     inputQueue, 
                                                     1,
                                                     outputQueue); 
            schedArray[i] = subConfig;
            gcRefs_OUT[i] = subConfig.recycleQueues;
        }
    }
    */

    *inputQueueRef_OUT = leaderInputQueue;
    *outputQueueRefs_OUT = leaderOutputQueues;
    return schedArray;
}

static uint32_t get_num_epochs(MVConfig config)
{
        uint32_t num_epochs;
        num_epochs = config.numTxns/config.epochSize;
        if (config.numTxns % config.epochSize) {
                num_epochs += 1;
        }
        return num_epochs;
}

/*
 * Pre-process each txn's read- and write-sets. 
 * 
 * XXX Ideally, this should happen _online_, and "route" transactions to the
 * appropriate concurrency control threads, but we're doing it offline because 
 * it's good enough for experiments.
 */
static void do_preprocessing(vector<CompositeKey> &keys, vector<int> &starts)
{
        CompositeKey mv_key;
        int indices[NUM_CC_THREADS], index, *ptr;
        uint32_t i, num_keys;

        for (i = 0; i < NUM_CC_THREADS; ++i)
                indices[i] = -1;
        num_keys = keys.size();
        for (i = 0; i < num_keys; ++i) {
                mv_key = keys[i];
                if (indices[mv_key.threadId] != -1) {
                        index = indices[mv_key.threadId];
                        ptr = &keys[index].next;
                } else {
                        ptr = &starts[mv_key.threadId];
                }
                indices[mv_key.threadId] = i;
                *ptr = i;
        }
}

static void convert_keys(mv_action *action, txn *txn)
{
        uint32_t i, num_reads, num_rmws, num_writes, num_entries;
        struct big_key *array;

        /* Alloc an array to poke txn information. */
        num_reads = txn->num_reads();
        num_rmws = txn->num_rmws();
        num_writes = txn->num_writes();
        if (num_reads >= num_rmws && num_reads >= num_writes) 
                num_entries = num_reads;
        else if (num_rmws >= num_writes) 
                num_entries = num_rmws;
        else 
                num_entries = num_writes;
        array = (struct big_key*)malloc(sizeof(struct big_key)*num_entries);
                
        /* Handle writes. */
        txn->get_writes(array);
        for (i = 0; i < num_writes; ++i)
                action->add_write_key(array[i].table_id, array[i].key, false);

        /* Handle rmws. */
        txn->get_rmws(array);
        for (i = 0; i < num_rmws; ++i)
                action->add_write_key(array[i].table_id, array[i].key, true);
        
        /* Handle reads. */
        txn->get_reads(array);
        for (i = 0; i < num_reads; ++i)
                action->add_read_key(array[i].table_id, array[i].key);
        if (num_rmws == 0 && num_writes == 0)
                action->__readonly = true;
        free(array);
}

static mv_action* generate_mv_action(txn *txn)
{
        mv_action *action;

        /* Get the transaction's rw-sets. */
        action = new mv_action(txn);
        txn->set_translator(action);
        convert_keys(action, txn);

        /* 
         * Pre-process rw-sets for more concurrency control phase parallelism. 
         */
        do_preprocessing(action->__writeset, action->__write_starts);
        do_preprocessing(action->__readset, action->__read_starts);
        action->setup_reverse_index();
        return action;        
}

static void postprocess_key(CompositeKey *key, CompositeKey ***prev, 
			    uint32_t partition_id)
{
	assert(partition_id < NUM_CC_THREADS && key->threadId < NUM_CC_THREADS);
	if (key->threadId == partition_id) {
		key->next_key = NULL;
		**prev = key;
		*prev = &(key->next_key);
		preproc_stats[partition_id] += 1;
	}
}

static void postprocess_partition(ActionBatch batch, uint32_t partition_id, 
				  CompositeKey **start_key)
{
	uint32_t i, j;
	CompositeKey *cur_key, **prev_key;
	mv_action *action;

	prev_key = start_key;
	for (i = 0; i < batch.numActions; ++i) {
		action = batch.actionBuf[i];
		for (j = 0; j < action->__readset.size(); ++j) {
			cur_key = &action->__readset[j];
			assert(cur_key->action == action);
			postprocess_key(cur_key, &prev_key, partition_id);
		}
		for (j = 0; j < action->__writeset.size(); ++j) {
			cur_key = &action->__writeset[j];
			assert(cur_key->action == action);
			postprocess_key(cur_key, &prev_key, partition_id);
		}
	}
}

/* Go through every action in the batch, and link read- and
   write-set entries according to partition
*/ 
static ActionBatch postprocess_batch(ActionBatch batch)
{
	uint32_t i;

	CompositeKey **start_keys;
	ActionBatch ret = batch;

	start_keys = (CompositeKey**)malloc(sizeof(CompositeKey*)*NUM_CC_THREADS);
	memset(start_keys, 0x0, sizeof(CompositeKey*)*NUM_CC_THREADS);
	for (i = 0; i < NUM_CC_THREADS; ++i) 
		postprocess_partition(batch, i, &start_keys[i]);	
	ret.start_keys = start_keys;
	return ret;
}

static ActionBatch mv_create_action_batch(MVConfig config,
                                          workload_config w_config,
                                          uint32_t epoch)
{
        ActionBatch batch;
        mv_action *action;
        txn *txn;
        uint32_t i;
        uint64_t timestamp;
        batch.numActions = config.epochSize;
        batch.actionBuf =
                (mv_action**)malloc(sizeof(mv_action*)*config.epochSize);
        assert(batch.actionBuf != NULL);
        for (i = 0; i < config.epochSize; ++i) {
                timestamp = CREATE_MV_TIMESTAMP(epoch, i);
                txn = generate_transaction(w_config);
                action = generate_mv_action(txn);
                action->__version = timestamp;
                batch.actionBuf[i] = action;
        }

	batch = postprocess_batch(batch);
	assert(batch.start_keys != NULL);
        return batch;
}

static void mv_setup_input_array(std::vector<ActionBatch> *input,
                                 MVConfig mv_config, workload_config w_config)
{
        uint32_t num_epochs;
        ActionBatch batch;
        uint32_t i;
        
        num_epochs = 2*get_num_epochs(mv_config);
        for (i = 0; i < num_epochs + MV_DRY_RUNS; ++i) {
                batch = mv_create_action_batch(mv_config, w_config, i+2);
                input->push_back(batch);
        }
        std::cerr << "Done setting up mv input!\n";
}

static ActionBatch generate_db(workload_config conf)
{
        txn **loader_txns;
        uint32_t num_txns, i;
        ActionBatch ret;

        uint64_t timestamp;

        loader_txns = NULL;
        num_txns = generate_input(conf, &loader_txns);
        assert(loader_txns != NULL);
        ret.numActions = num_txns;
        ret.actionBuf = (mv_action**)malloc(sizeof(mv_action*)*num_txns);
        for (i = 0; i < num_txns; ++i) {
                ret.actionBuf[i] = generate_mv_action(loader_txns[i]);
                timestamp = CREATE_MV_TIMESTAMP(1, i);
                ret.actionBuf[i]->__version = timestamp;
        }
	ret = postprocess_batch(ret);
        return ret;
}
 
static void write_results(MVConfig config, timespec elapsed_time)
{
        uint32_t num_epochs;
        double elapsed_milli;
        std::ofstream result_file;
        num_epochs = get_num_epochs(config);
        elapsed_milli =
                1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
        std::cerr << "Number of txns: " << config.numTxns << "\n";
        std::cerr << "Time: " << elapsed_milli << "\n";
        result_file.open("results.txt", std::ios::app | std::ios::out);
        result_file << "mv ";
        result_file << "time:" << elapsed_milli << " ";
        result_file << "txns:" << (num_epochs)*config.epochSize << " ";
        result_file << "ccthreads:" << config.numCCThreads << " ";
        result_file << "workerthreads:" << config.numWorkerThreads << " ";
        result_file << "records:" << config.numRecords << " ";
        result_file << "read_pct:" << config.read_pct << " ";
        if (config.experiment == 0) {
                result_file << "10rmw ";
        } else if (config.experiment == 1) {
                result_file << "8r2rmw ";
        } else if (config.experiment == 2) {
                result_file << "5w ";
        } else if (config.experiment < 5) {
                result_file << "small_bank ";
        }        
        if (config.distribution == 0) {
                result_file << "uniform";
        } else if (config.distribution == 1) {
                result_file << "zipf theta:" << config.theta;
        }
        result_file << "\n";
        result_file.close();
        
}

static timespec run_experiment(SimpleQueue<ActionBatch> *input_queue,
                               SimpleQueue<ActionBatch> *output_queue,
                               std::vector<ActionBatch> inputs,
                               uint32_t num_workers)
{
        uint32_t num_batches, num_wait_batches, i, j;
        struct timespec elapsed_time, end_time, start_time;
        num_batches = inputs.size();
        num_wait_batches = (num_batches - MV_DRY_RUNS) / 2;

        barrier();
        for (i = 0; i < MV_DRY_RUNS; ++i)
                input_queue->EnqueueBlocking(inputs[i]);
        for (i = 0; i < MV_DRY_RUNS; ++i)
                for (j = 0; j < num_workers; ++j)
                        (&output_queue[j])->DequeueBlocking();
        barrier();

        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
        barrier();                
        for (i = MV_DRY_RUNS; i < num_batches; ++i) {
                input_queue->EnqueueBlocking(inputs[i]);
        }
        barrier();
        for (i = MV_DRY_RUNS; i < num_wait_batches; ++i) {
                for (j = 0; j < num_workers; ++j) 
                        (&output_queue[j])->DequeueBlocking();
        }
        barrier();
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
        barrier();
        elapsed_time = diff_time(end_time, start_time);
        std::cerr << "Done running Bohm experiment!\n";
        return elapsed_time;
}

static void init_database(MVConfig config,
                          workload_config w_conf,
                          SimpleQueue<ActionBatch> *input_queue,
                          SimpleQueue<ActionBatch> *output_queue,
                          hybrid_worker **db_threads)
                          
{
        uint32_t i;
        ActionBatch init_batch;
        int pin_success, num_threads;
        
        num_threads = config.numCCThreads + config.numWorkerThreads;
        pin_success = pin_thread(79);
        assert(pin_success == 0);
        init_batch = generate_db(w_conf);
        for (i = 0; i < num_threads; ++i) {
            db_threads[i]->Run();
            db_threads[i]->WaitInit();
        }

        input_queue->EnqueueBlocking(init_batch);
        for (i = 0; i < num_threads; ++i) 
                (&output_queue[i])->DequeueBlocking();
        barrier();
        std::cerr << "Done loading the database!\n";
        return;
}

static MVSchedulerConfig* setup_scheduler_threads(MVConfig config,
                                                  SimpleQueue<ActionBatch> **sched_input,
                                                  SimpleQueue<ActionBatch> **sched_output,
                                                  SimpleQueue<MVRecordList> ***gc_queues)
{
        uint64_t stickies_per_thread;
        uint32_t num_tables;
        MVSchedulerConfig *sched_configs;
        int num_procs = config.numCCThreads + config.numWorkerThreads;

        if (config.experiment < 3) {
                stickies_per_thread = (((uint64_t)1)<<24);
                num_tables = 1;
        } else if (config.experiment < 5) {
                stickies_per_thread = (((uint64_t)1)<<24);
                num_tables = 2;
        } else {
                assert(false);
        }
        sched_configs = SetupSchedulers(num_procs, 
                                        sched_input,
                                        sched_output, 
                                        num_procs + 1,
                                        stickies_per_thread, 
                                        num_tables,
                                        config.numRecords, 
                                        gc_queues);
                                        
        assert(sched_configs != NULL);
        assert(*sched_input != NULL);
        assert(*sched_output != NULL);
        std::cerr << "Done setting up scheduler threads!\n";
        std::cerr << "Num scheduler threads:";
        std::cerr << MVScheduler::NUM_CC_THREADS << "\n";
        return sched_configs;
}

static ExecutorConfig* setup_executors(MVConfig config,
                                  SimpleQueue<ActionBatch> *sched_outputs,
                                  SimpleQueue<ActionBatch> *output_queue,
                                  SimpleQueue<MVRecordList> ***gc_queues)
{
        uint32_t queues_per_table, queues_per_cc_thread;
        ExecutorConfig *execs;
        int num_procs = config.numWorkerThreads + config.numCCThreads; 

        queues_per_table = num_procs;
        queues_per_cc_thread = num_procs; 
        execs = SetupExecutors(num_procs,
                               queues_per_table,
                               sched_outputs, output_queue,
                               queues_per_cc_thread, gc_queues);
        std::cerr << "Done setting up executors!\n";
        return execs;
}

void do_mv_experiment(MVConfig mv_config, workload_config w_config)
{
        MVSchedulerConfig *sched_configs;
        ExecutorConfig *exec_configs;
        hybrid_worker **db_workers;

        SimpleQueue<ActionBatch> *schedInputQueue;
        SimpleQueue<ActionBatch> *schedOutputQueues;
        SimpleQueue<MVRecordList> **schedGCQueues[mv_config.numCCThreads + mv_config.numWorkerThreads];
        SimpleQueue<ActionBatch> *outputQueue;
        std::vector<ActionBatch> input_placeholder;
        timespec elapsed_time;
        uint32_t num_threads, i;

        /* 
         * XXX Need this for copying old versions of records if a txn performs 
         * an RMW. Ideally, we need to separate record allocation from version 
         * allocation to make it work properly. "Engineering effort". See 
         * src/executor.cc.
         */
        if (w_config.experiment < 3)
                GLOBAL_RECORD_SIZE = 1000;
        else
                GLOBAL_RECORD_SIZE = 8;
	
	memset(preproc_stats, 0x0, sizeof(uint64_t)*160);        
        num_threads = (uint32_t)(mv_config.numCCThreads + mv_config.numWorkerThreads);
        MVScheduler::NUM_CC_THREADS = num_threads;
        NUM_CC_THREADS = num_threads;
        assert(mv_config.distribution < 2);
        outputQueue = SetupQueuesMany<ActionBatch>(INPUT_SIZE,
                                                   num_threads,
                                                   71);

        sched_configs = setup_scheduler_threads(mv_config, &schedInputQueue,
                                                &schedOutputQueues,
                                                schedGCQueues);


        mv_setup_input_array(&input_placeholder, mv_config, w_config);
	
	for (i = 0; i < NUM_CC_THREADS; ++i) {
		std::cout << "Thread: " << i << ", " << preproc_stats[i] << "\n";
	}
        exec_configs = setup_executors(mv_config, schedOutputQueues, outputQueue,
                                      schedGCQueues);
        db_workers = setup_threads(mv_config, sched_configs, exec_configs);
        init_database(mv_config, w_config, schedInputQueue, outputQueue,
                      db_workers); 
        pin_memory();
        elapsed_time = run_experiment(schedInputQueue,  //&schedOutputQueues[config.numWorkerThreads],
                                      outputQueue,
                                      input_placeholder,// 1);
                                      mv_config.numWorkerThreads);
        write_results(mv_config, elapsed_time);
}
