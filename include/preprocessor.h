#ifndef         PREPROCESSOR_H_
#define         PREPROCESSOR_H_

#include <mv_action.h>
#include <runnable.hh>
#include <concurrent_queue.h>
#include <numa.h>
#include <mv_table.h>

extern uint64_t recordSize;

class CompositeKey;
class mv_action;
class MVRecordAllocator;
class MVTablePartition;


/*
 * An MVActionHasher is the first stage of the transaction processing pipleline.
 * Its job is to take a batch of transactions as input, and assign each key of 
 * each transaction to a concurrency control worker thread. We hash keys in this
 * stage because it reduces the amount of serial work that must be perfomed by 
 * the concurrency control stage.
 */
class MVActionHasher : public Runnable {
 private:
  
  uint32_t numHashers;

  // A batch of transactions is input through this queue.
  SimpleQueue<ActionBatch> *inputQueue;
  
  // Once a batch of transactions is completed, output to this queue.
  SimpleQueue<ActionBatch> *outputQueue;  

  // 
  SimpleQueue<ActionBatch> **leaderEpochStartQueues;
  SimpleQueue<ActionBatch> **leaderEpochStopQueues;
  
  SimpleQueue<ActionBatch> *subordInputQueue;
  SimpleQueue<ActionBatch> *subordOutputQueue;

 protected:
  
  virtual void StartWorking();
  
  virtual void Init();
  
  static inline void ProcessAction(mv_action *action, uint32_t epoch, 
                                   uint32_t txnCounter);

 public:
  
  // Override the default allocation mechanism. We want all thread local memory
  // allocations to be 
  // 
  void* operator new(std::size_t sz, int cpu);
  
  // Constructor
  //
  // param numThreads: Number of concurrency control threads (in the next stage)
  // param inputQueue: Queue through which batches are input
  // param outputQueue: Queue through which batches are output
  MVActionHasher(int cpuNumber,
                 SimpleQueue<ActionBatch> *inputQueue, 
                 SimpleQueue<ActionBatch> *outputQueue);
};

struct MVSchedulerConfig {
  int cpuNumber;
  uint32_t threadId;
  size_t allocatorSize;         // Scheduler thread's local sticky allocator
  uint32_t numTables;           // Number of tables in the system
  size_t *tblPartitionSizes;    // Size of each table's partition
  
  uint32_t numOutputs;
        
  uint32_t numSubords;
  uint32_t numRecycleQueues;

  SimpleQueue<ActionBatch> *inputQueue;
  SimpleQueue<ActionBatch> *outputQueues;
  SimpleQueue<ActionBatch> **pubQueues;
  SimpleQueue<ActionBatch> **subQueues;
  SimpleQueue<MVRecordList> **recycleQueues;

        int worker_start;
        int worker_end;
        
  /*
  // Coordination queues required by the leader thread.
  SimpleQueue<ActionBatch> *leaderInputQueue;
  SimpleQueue<ActionBatch> *leaderOutputQueue;
  SimpleQueue<ActionBatch> **leaderEpochStartQueues;
  SimpleQueue<ActionBatch> **leaderEpochStopQueues;
    
  // Coordination queues required by the subordinate threads.
  SimpleQueue<ActionBatch> *subordInputQueue;
  SimpleQueue<ActionBatch> *subordOutputQueue;
  */
};

/*
 * MVScheduler implements scheduling logic. The scheduler is partitioned across 
 * several physical cores.
 */
class MVScheduler : public Runnable {
        friend class SchedulerTest;
        
 private:
        static inline uint32_t GetCCThread(CompositeKey key);

    MVSchedulerConfig config;
    MVRecordAllocator *alloc;

    MVTablePartition **partitions;

    uint32_t epoch;
    uint32_t txnCounter;
    uint64_t txnMask;

    uint32_t threadId;

 protected:
        virtual void StartWorking();
        void ProcessWriteset(mv_action *action);
        void ScheduleTransaction(mv_action *action);
        //    void Leader(uint32_t epoch);
        //    void Subordinate(uint32_t epoch);
    virtual void Init();
    virtual void Recycle();
 public:
    
    void* operator new (std::size_t sz, int cpu) {
            return alloc_mem(sz, cpu);
            /*
      int numa_node = numa_node_of_cpu(cpu);
      numa_set_strict(1);
      void *buf = numa_alloc_onnode(sz, numa_node);
      if (buf == NULL) {
        return buf;
      }
      if (mlock(buf, sz) != 0) {
        numa_free(buf, sz);
        std::cout << "mlock couldn't pin memory to RAM!\n";
        return NULL;
      } 
      else {
        return buf;
      }
            */
    }

        static uint32_t NUM_CC_THREADS;
        MVScheduler(MVSchedulerConfig config);
};


#endif          /* PREPROCESSOR_H_ */
