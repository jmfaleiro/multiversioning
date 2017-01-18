#include <mv_record.h>
#include <cstdlib>
#include <cpuinfo.h>
#include <iostream>

uint64_t _MVRecord_::INFINITY = 0xFFFFFFFFFFFFFFFF;

MVRecordAllocator::MVRecordAllocator(uint64_t size, int cpu)
{
    if (size < 1) {
        size = 1;
    }
    MVRecord *data = (MVRecord*)alloc_mem(size, cpu);
    assert(data != NULL);
    memset(data, 0x0, size);
        
    this->size = size;
    this->count = 0;
    uint64_t numRecords = size/sizeof(MVRecord);
  
    uint64_t recordDataSize = recordSize*numRecords;
    char *recordData = (char*)alloc_interleaved_all(recordDataSize);

    assert(recordData != NULL);
    memset(recordData, 0xA3, recordDataSize);

    for (uint64_t i = 0; i < numRecords; ++i) {
        data[i].allocLink = &data[i+1];
        data[i].value = (Record*)(&recordData[i*recordSize]);
        data[i].writer = NULL;
        this->count += 1;
    }
    data[numRecords-1].allocLink = NULL;
    freeList = data;
}

void MVRecordAllocator::WriteAllocator() 
{
}

bool MVRecordAllocator::GetRecord(MVRecord **OUT_recordPtr) 
{
    if (freeList == NULL) {
        std::cout << "Free list empty: " << count << "\n";
        *OUT_recordPtr = NULL;
        return false;
    }
        
    MVRecord *ret = freeList;
    freeList = freeList->allocLink;

    // Set up the MVRecord to return.
    //  memset(ret, 0xA3, sizeof(MVRecord));
    ret->link = NULL;
    ret->recordLink = NULL;
    ret->allocLink = NULL;
    ret->epoch_ancestor = NULL;
    ret->writer = NULL;
    //  ret->value = NULL;
    *OUT_recordPtr = ret;
    count -= 1;
    return true;
}

/*
 *
 */
void MVRecordAllocator::ReturnMVRecords(MVRecordList recordList) 
{
    // XXX Should we validate that MVRecords are properly linked?
    if (recordList.tail != &recordList.head) {
        *(recordList.tail) = freeList;
        freeList = recordList.head;
        this->count += recordList.count;
    }
    else {
        assert(recordList.head == NULL);
    }
}



