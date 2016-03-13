#ifndef 	MCS_H_
#define 	MCS_H_

#include <cpuinfo.h>
#include <machine.h>
#include <stdint.h>
#include <cstddef>

struct mcs_struct {
        volatile mcs_struct 	**_tail_ptr;
        volatile uint64_t 	_is_held;
        volatile mcs_struct	*_next;
} __attribute__((__packed__, __aligned__(CACHE_LINE)));

class mcs_mgr {
 private:
        mcs_struct 	*_free_list;

 public:
        void* operator new (std::size_t sz, int cpu) {
                return alloc_mem(sz, cpu);
        }

        mcs_mgr(uint32_t num_locks, int cpu);
        mcs_struct* get_struct();
        void return_struct(mcs_struct *m_struct);

        static void lock(mcs_struct *lock);
        static void unlock(mcs_struct *lock);
};

#endif 		// MCS_H_
