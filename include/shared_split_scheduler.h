#ifndef 	SHARED_SPLIT_SCHEDULER_H_
#define 	SHARED_SPLIT_SCHEDULER_H_

#include <runnable.hh>
#include <table.h>
#include <split_action.h>
#include <split_executor.h>

struct shared_scheduler_config {
        int 			_cpu;
        uint32_t 		_partition_id;
        uint32_t 		_num_partitions;
        uint32_t 		_num_tables;
        Table			**_tables;
        volatile uint64_t	*_low_watermark_ptr;        
};

class dep_manager {
 private:        
        split_dep 		*_dep_array;
        uint64_t 		_dep_index;
        uint64_t 		_reclaimed_watermark;
        uint64_t 		_num_deps;

        uint64_t 		*_range_ptrs;
        uint64_t 		_last_reclaimed;
        uint64_t 		_epoch;
        volatile uint64_t	*_low_watermark_ptr;
        uint64_t 		_num_ranges;

 public:
        dep_manager(int cpu, uint64_t array_sz);

        split_dep* 	get_dep();
        void 		gc();
        void 		finish_epoch();
};

class shared_split_scheduler : public Runnable {
 private:

        dep_manager 		*_dep_allocator;
        


 protected:
        
        void StartWorking();
        void Init();
        
 public:
        shared_split_scheduler();
};

#endif 		// SHARED_SPLIT_SCHEDULER_H_
