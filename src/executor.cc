#include <executor.h>
#include <algorithm>

/*
 * Each executor will use about 512+256M of memory.
 */
#define 	ALLOCATOR_SIZE 		((1<<29) + (1<<28))

extern uint32_t GLOBAL_RECORD_SIZE;

PendingActionList::PendingActionList(uint32_t freeListSize) {
  freeList = (ActionListNode*)malloc(sizeof(ActionListNode)*freeListSize);
  memset(freeList, 0x00, sizeof(ActionListNode)*freeListSize);
  
  for (uint32_t i = 0; i < freeListSize; ++i) {
    freeList[i].next = &freeList[i+1];
  }
  freeList[freeListSize-1].next = NULL;
  this->head = NULL;
  this->tail = NULL;
  this->cursor = NULL;
  this->size = 0;
}

inline void PendingActionList::EnqueuePending(mv_action *action) {
  assert((head != NULL && tail != NULL && size > 0) || 
         (head == NULL && tail == NULL && size == 0));

  assert(freeList != NULL);
  assert(action != NULL);

  ActionListNode *node = freeList;
  freeList = freeList->next;  
  node->action = action;
  node->next = NULL;  
  node->prev = NULL;

  if (tail == NULL) {
    assert(head == NULL && size == 0);
    node->prev = NULL;
    head = node;  
    tail = node;
  }
  else {
    assert(head != NULL && size > 0);
    tail->next = node;
    node->prev = tail;
  }
  this->size += 1;
  tail = node;

  assert((head != NULL && tail != NULL && size > 0) || 
         (head == NULL && tail == NULL && size == 0));
}

inline void PendingActionList::DequeuePending(ActionListNode *node) {
  assert((head != NULL && tail != NULL && size > 0) || 
         (head == NULL && tail == NULL && size == 0));
  assert(node != cursor);
  assert(size > 0);

  if (node->next == NULL) {
    tail = node->prev;
  }
  else {
    node->next->prev = node->prev;
  }
  
  if (node->prev == NULL) {
    head = node->next;
  }
  else {
    node->prev->next = node->next;
  }
  
  /*
  if (node->next == NULL && node->prev == NULL) {
    head = NULL;
    tail = NULL;
  }
  else if (node->next == NULL) {
    node->prev->next = NULL;
    tail = node->prev;
  }
  else if (node->prev == NULL) {
    node->next->prev = NULL;
    head = node->next;
  }
  else {
    node->prev->next = node->next;
    node->next->prev = node->prev;
  }
  */

  node->prev = NULL;
  node->action = NULL;
  node->next = freeList;
  freeList = node;
  this->size -= 1;

  assert((head != NULL && tail != NULL && size > 0) || 
         (head == NULL && tail == NULL && size == 0));
}

inline void PendingActionList::ResetCursor() {
  cursor = head;
}

inline ActionListNode* PendingActionList::GetNext() {
  ActionListNode *temp = cursor;
  if (cursor != NULL) {
    cursor = cursor->next;
  }
  return temp;
}

inline bool PendingActionList::IsEmpty() {
  assert((head == NULL && size == 0) || (head != NULL && size > 0));
  return head == NULL;
}

inline uint32_t PendingActionList::Size() {
  return this->size;
}

Executor::Executor(ExecutorConfig cfg) : Runnable (cfg.cpu) {        
  this->config = cfg;
    this->counter = 0;
  /*
  this->allocators = 

    (RecordAllocator**)malloc(sizeof(RecordAllocator*)*config.numTables);
  for (uint32_t i = 0; i < config.numTables; ++i) {
    uint32_t recSize = config.recordSizes[i];
    uint32_t allocSize = config.allocatorSizes[i];
    this->allocators[i] = 
      new (config.cpu) RecordAllocator(recSize, allocSize, config.cpu);
  }
    */
  this->pendingList = new (config.cpu) PendingActionList(1000);
  this->garbageBin = new (config.cpu) GarbageBin(config.garbageConfig);
  //  this->pendingGC = new (config.cpu) PendingActionList(20000);
  /*
  char *temp_bufs = (char*)alloc_mem(1000*250000, config.cpu);
  memset(temp_bufs, 0x0, 1000*250000);
  this->bufs = (void**)alloc_mem(sizeof(void*)*250000, config.cpu);
  for (uint32_t i = 0; i < 250000; ++i)
          this->bufs[i] = &temp_bufs[1000*i];
  std::random_shuffle(&this->bufs[0], &this->bufs[250000-1]);
  this->buf_ptr = 0;
  */

}

void Executor::Init() 
{
        this->allocator = new (config.cpu) RecordAllocator(config.threadId, 
                                                           config.cpu);
}

uint64_t Executor::next_ptr()
{
        uint64_t ret = buf_ptr;
        buf_ptr = (buf_ptr + 1) % 250000;
        return ret;
}

void Executor::LeaderFunction() {
  assert(config.threadId == 0);
  //  ActionBatch dummy = { NULL, 0 };
  volatile uint32_t minEpoch = *config.epochPtr;
  //        std::cout << "0:" << minEpoch << "\n";
  for (uint32_t i = 1; i < config.numExecutors; ++i) {
    barrier();
    volatile uint32_t temp = config.epochPtr[i];
    //          std::cout << i << ":" << temp << "\n";
    barrier();
        
    if (temp < minEpoch) {
      minEpoch = temp;
    }
  }
      
  uint32_t prev;
  barrier();
  prev = *config.lowWaterMarkPtr;
  if (minEpoch > prev) {
    *config.lowWaterMarkPtr = minEpoch;
    //          std::cout << "LowWaterMark: " << minEpoch << "\n";
  }
  barrier();

  /*
  for (uint32_t i = 0; i < minEpoch - prev; ++i) {
    config.outputQueue->EnqueueBlocking(dummy);
  }
  */
}
/*
static void wait() 
{
        volatile uint64_t temp = 1;
        while (temp == 1)
                ;
}
*/
void Executor::StartWorking() 
{
        uint32_t epoch = 1;

        //  ActionBatch dummy;
        while (true) {
                // Process the new batch of transactions
    
                if (config.threadId == 0) {
                        ActionBatch batch;
                        while (!config.inputQueue->Dequeue(&batch)) {
                                LeaderFunction();
                        }
                        ProcessBatch(batch);
                }
                else {
                        ActionBatch batch = config.inputQueue->DequeueBlocking();    
                        ProcessBatch(batch);    
                }

                barrier();
                *config.epochPtr = epoch;
                barrier();
    
                /*
                  if (DoPendingGC()) {
                  // Tell other threads that this epoch is finished
                  barrier();
                  *config.epochPtr = epoch;
                  barrier();
                  }
                */
    
                // If this is the leader thread, try to advance the low-water mark to 
                // trigger garbage collection
                if (config.threadId == 0) {
                        LeaderFunction();
                }


                // Try to return records that are no longer visible to their owners
                garbageBin->FinishEpoch(epoch);
    
                // Check if there's any garbage we can recycle. Insert into our record 
                // allocators.
                //    RecycleData();

                epoch += 1;
        }
}
/*
uint64_t GetEpoch()
{
        return *config.epochPtr;
}
*/
// Check if other worker threads have returned data to be recycled.
void Executor::RecycleData() 
{
        uint32_t num_workers, i;
        RecordList recycled;

        num_workers = config.numExecutors;
        for (i = 0; i < num_workers; ++i) 
                while (config.recycleQueues[i].Dequeue(&recycled)) 
                        allocator->Recycle(recycled);
}

void Executor::ExecPending() {
  pendingList->ResetCursor();
  for (ActionListNode *node = pendingList->GetNext(); node != NULL; 
       node = pendingList->GetNext()) {
    if (ProcessSingle(node->action)) {
      pendingList->DequeuePending(node);
    }
  }
}

void Executor::ProcessBatch(const ActionBatch &batch) {
        /*
        uint32_t div = batch.numActions/(uint32_t)config.numExecutors;
        uint32_t remainder = batch.numActions % (uint32_t)config.numExecutors;
        uint32_t start = div*config.threadId;
        uint32_t end = div*(config.threadId+1);
        if (config.threadId == config.numExecutors-1) {
                end += remainder;
        }
        for (uint32_t i = end; i >= start; --i) {
                
        }
        */
        for (int i = config.threadId; i < (int)batch.numActions;
             i += config.numExecutors) {
                while (pendingList->Size() > 0) {
                        ExecPending();
                }

                mv_action *cur = batch.actionBuf[i];
                if (!ProcessSingle(cur)) {
                        pendingList->EnqueuePending(cur);
                }
        }

  // DEBUGGIN
  /*
  pendingList->ResetCursor();
  for (ActionListNode *node = pendingList->GetNext(); node != NULL;
       node = pendingList->GetNext()) {
    assert(node->action != NULL);
  }
  */

  while (!pendingList->IsEmpty()) {
    ExecPending();
  }

  ActionBatch dummy = {NULL, 0};
  config.outputQueue->EnqueueBlocking(dummy);  
}

// Returns the epoch of the oldest pending record.
uint32_t Executor::DoPendingGC() {
        static uint64_t mask = 0xFFFFFFFF00000000;
  pendingGC->ResetCursor();
  for (ActionListNode *node = pendingGC->GetNext(); node != NULL; 
       node = pendingGC->GetNext()) {
    assert(node->action != NULL);
    if (ProcessSingleGC(node->action)) {      
      pendingGC->DequeuePending(node);
    }
  }

  if (pendingGC->IsEmpty()) {
    return 0xFFFFFFFF;
  }
  else {
    pendingGC->ResetCursor();
    ActionListNode *node = pendingGC->GetNext();
    assert(node != NULL && node->action != NULL);
    return ((node->action->__version & mask) >> 32);
  }
}

bool Executor::ProcessSingleGC(mv_action *action) {
  assert(action->__state == SUBSTANTIATED);
  uint32_t numWrites = action->__writeset.size();
  bool ret = true;

  // Garbage collect the previous versions
  for (uint32_t i = 0; i < numWrites; ++i) {
    MVRecord *previous = action->__writeset[i].value->recordLink;
    if (previous != NULL) {
      ret &= (previous->writer == NULL || 
              previous->writer->__state == SUBSTANTIATED);
    }
  }
  
  if (ret) {
    for (uint32_t i = 0; i < numWrites; ++i) {
      MVRecord *previous = action->__writeset[i].value->recordLink;
      if (previous != NULL) {
        
        // Since the previous txn has been substantiated, the record's value 
        // shouldn't be NULL.
        assert(previous->value != NULL);
        garbageBin->AddRecord(previous->value);
        //        garbageBin->AddMVRecord(action->__writeset[i].threadId, previous);
      }
    }
  }
  
  return ret;
}

bool Executor::ProcessSingle(mv_action *action) {
        assert(action != NULL);
        volatile uint64_t state;
        barrier();
        state = action->__state;
        barrier();
        if (state != SUBSTANTIATED) {
                if (state == STICKY &&
                    cmp_and_swap(&action->__state, STICKY, PROCESSING)) {
                        if (ProcessTxn(action)) {
                                return true;
                        } else {
                                xchgq(&action->__state, STICKY);
                                return false;
                        }
                } else {      // cmp_and_swap failed
                        return false;
                }
        }
        else {        // action->state == SUBSTANTIATED
                return true;
        }
}

void Executor::init_txn(mv_action *action)
{
        uint32_t i, num_writes;
        MVRecord *prev;
        void *new_data, *old_data;
        
        num_writes = action->__writeset.size();
        for (i = 0; i < num_writes; ++i) {
                if (action->__writeset[i].is_rmw == true) {
                        prev = action->__writeset[i].value->recordLink;
                        new_data = action->__writeset[i].value->value;
                        old_data = prev->value;
                        memcpy(new_data, old_data, GLOBAL_RECORD_SIZE);
                        action->__writeset[i].initialized = true;  
                }              
        }
}

bool Executor::check_ready(mv_action *action)
{
        uint32_t num_reads, num_writes, i;
        bool ready;
        mv_action *depend_action;
        MVRecord *prev;
        uint32_t *read_index, *write_index;

        ready = true;
        num_reads = action->__readset.size();
        num_writes = action->__writeset.size();
        read_index = &action->read_index;
        write_index = &action->write_index;
        for (; *read_index < num_reads; *read_index += 1) {
                i = *read_index;

                assert(action->__readset[i].value != NULL);
                depend_action = action->__readset[i].value->writer;
                if (depend_action != NULL &&
                    depend_action->__state != SUBSTANTIATED &&
                    !ProcessSingle(depend_action)) {
                        ready = false;
                        break;
                }
        }
        for (; *write_index < num_writes; *write_index += 1) {
                i = *write_index;
                assert(action->__writeset[i].value != NULL);
                if (action->__writeset[i].is_rmw) {
                        prev = action->__writeset[i].value->recordLink;
                        assert(prev != NULL);
                        depend_action = prev->writer;
                        if (depend_action != NULL &&
                            depend_action->__state != SUBSTANTIATED && 
                            !ProcessSingle(depend_action)) {
                                ready = false;
                                break;
                        } 
                }
        }
        return ready;
}

bool Executor::ProcessTxn(mv_action *action) {
        assert(action != NULL && action->__state == PROCESSING);
        if (action->__readonly == true) {
                assert(action->__writeset.size() == 0);
                uint32_t num_reads = action->__readset.size();
                uint64_t action_epoch = GET_MV_EPOCH(action->__version);
                for (uint32_t i = 0; i < num_reads; ++i) {
                        MVRecord *rec = action->__readset[i].value;
                        MVRecord *snapshot;
                  if (action_epoch == GET_MV_EPOCH(rec->createTimestamp)) {
                          snapshot = rec->epoch_ancestor;
                  } else {
                          snapshot = rec;
                  }
                  barrier();
                  void *value_ptr = snapshot->value;
                  barrier();
                  if (value_ptr == NULL)
                          return false;
                }
                action->exec = this;
                action->Run();
                xchgq(&action->__state, SUBSTANTIATED);
                return true;
        }
        uint32_t numWrites = action->__writeset.size();
        bool ready = check_ready(action);
        if (ready == false) {
                return false;
        }
        action->exec = this;
        init_txn(action);
        action->Run();
        
        xchgq(&action->__state, SUBSTANTIATED);

        for (uint32_t i = 0; i < numWrites; ++i) {
                MVRecord *previous = action->__writeset[i].value->recordLink;
                Record *prev_record;
                if (previous != NULL) {
                        garbageBin->AddMVRecord(action->__writeset[i].threadId, previous);
                        prev_record = previous->value;
                        garbageBin->AddRecord(prev_record);
                }
        }        
        return ready;
}

GarbageBin::GarbageBin(GarbageBinConfig config) {
  assert(sizeof(MVRecordList) == sizeof(RecordList));
  this->config = config;
  this->snapshotEpoch = 0;

  // total number of structs
  uint32_t numStructs = (config.numCCThreads + config.numWorkers);
  uint32_t ccOffset = config.numCCThreads*sizeof(MVRecordList);
  uint32_t workerOffset = 
    config.numWorkers*sizeof(MVRecordList);

  // twice #structs: one for live, one for snapshot
  void *data = alloc_mem(2*numStructs*sizeof(MVRecordList), config.cpu);
  memset(data, 0x00, 2*numStructs*sizeof(MVRecordList));
  
  this->curStickies = (MVRecordList*)data;
  this->snapshotStickies = (MVRecordList*)((char*)data + ccOffset);
  for (uint32_t i = 0; i < 2*config.numCCThreads; ++i) {
    curStickies[i].tail = &curStickies[i].head;
    curStickies[i].head = NULL;
    curStickies[i].count = 0;
  }
  
  this->curRecords = (RecordList*)((char*)data + 2*ccOffset);
  this->snapshotRecords = (RecordList*)((char*)data + 2*ccOffset+workerOffset);
  for (uint32_t i = 0; i < 2*config.numWorkers; ++i) {
    curRecords[i].tail = &curRecords[i].head;
    curRecords[i].head = NULL;
    curRecords[i].count = 0;
  }
}

void GarbageBin::AddMVRecord(uint32_t ccThread, MVRecord *rec) {
  rec->allocLink = NULL;
  *(curStickies[ccThread].tail) = rec;
  curStickies[ccThread].tail = &rec->allocLink;
  curStickies[ccThread].count += 1;
  assert(curStickies[ccThread].head != NULL);
}

void GarbageBin::AddRecord(Record *rec) 
{
        uint32_t workerThread;
        
        workerThread = rec->thread_id;
        rec->next = NULL;
        *(curRecords[workerThread].tail) = rec;
        curRecords[workerThread].tail = &rec->next;  
        curRecords[workerThread].count += 1;
        assert(curRecords[workerThread].head != NULL);
}

void GarbageBin::ReturnGarbage() {
  for (uint32_t i = 0; i < config.numCCThreads; ++i) {
    // *curStickies[i].tail = NULL;

    // Try to enqueue garbage. If enqueue fails, we'll just try again during the
    // next call.
    if (snapshotStickies[i].head != NULL) {
      if (!config.ccChannels[i]->Enqueue(snapshotStickies[i])) {
        *(curStickies[i].tail) = snapshotStickies[i].head;
        curStickies[i].tail = snapshotStickies[i].tail;
        curStickies[i].count += snapshotStickies[i].count;
      }
      else {
        //        std::cout << "Recycle!\n";
      }
    }
    snapshotStickies[i] = curStickies[i];
    curStickies[i].head = NULL;
    curStickies[i].tail = &curStickies[i].head;
    curStickies[i].count = 0;
  }

  for (uint32_t i = 0; i < config.numWorkers; ++i) {

      // Same logic as "stickies"
      if (snapshotRecords[i].head != NULL) {
        if (!config.workerChannels[i]->Enqueue(snapshotRecords[i])) {
          *(curRecords[i].tail) = snapshotRecords[i].head;
          curRecords[i].tail = snapshotRecords[i].tail;
          curRecords[i].count += snapshotRecords[i].count;
        }
      }
      snapshotRecords[i] = curRecords[i];
      curRecords[i].head = NULL;
      curRecords[i].tail = &curRecords[i].head;
      curRecords[i].count = 0;
  }
}

void GarbageBin::FinishEpoch(uint32_t epoch) 
{
        barrier();
        uint32_t lowWatermark = *config.lowWaterMarkPtr;
        barrier();
  
        if (lowWatermark >= snapshotEpoch) {
                ReturnGarbage();
                snapshotEpoch = epoch;
        }
}

RecordAllocator::RecordAllocator(uint32_t thread_id, int cpu)
{
        Record *cur, *next;
        char *data;
        uint64_t num_records;
        uint64_t i;
        uint32_t record_sz;

        record_sz = sizeof(Record) + recordSize;
        data = (char*)alloc_mem(ALLOCATOR_SIZE, cpu);
        memset(data, 0x0, ALLOCATOR_SIZE);
        num_records = ALLOCATOR_SIZE / record_sz;
        for (i = 0; i < num_records; ++i) {
                cur = (Record*)&data[i*record_sz];
                next = (Record*)&data[(i+1)*record_sz];
                cur->next = next;
                cur->thread_id = thread_id;
        }
        ((Record*)&data[(num_records-1)*record_sz])->next = NULL;
        freeList = (Record*)data;
}

bool RecordAllocator::GetRecord(Record **OUT_REC) 
{
        if (freeList != NULL) {
                Record *temp = freeList;
                freeList = freeList->next;
                temp->next = NULL;
                *OUT_REC = temp;    
                return true;
        } else {
                *OUT_REC = NULL;
                return false;
        }
}

void RecordAllocator::FreeSingle(Record *rec) 
{
        rec->next = freeList;
        freeList = rec;
}

void RecordAllocator::Recycle(RecordList recList) 
{
        *(recList.tail) = freeList;
        freeList = recList.head;
}
