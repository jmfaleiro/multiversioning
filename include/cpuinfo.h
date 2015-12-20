#ifndef _CPUINFO_H
#define _CPUINFO_H

#include <pthread.h>
#include <numa.h>
#include <sys/mman.h>

#ifndef MAP_HUGETLB
#define MAP_HUGETLB 0x40000
#endif

#define MMAP_FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB)
#define MMAP_PROT (PROT_READ | PROT_WRITE)
#define MMAP_ADDR (void *)(0x0UL)

void alloc_interleave_all();

void init_cpuinfo();

int get_num_cpus();

int get_cpu(int index, int striped);

int pin_thread(int cpu);

void* alloc_mem(size_t size, int cpu);

void* alloc_huge(size_t size);

void* alloc_interleaved(size_t size, int startCpu, int endCpu);

void* alloc_interleaved_all(size_t size);

#endif
