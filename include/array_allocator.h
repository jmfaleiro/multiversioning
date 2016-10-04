#ifndef 	ARRAY_ALLOCATOR_H_
#define 	ARRAY_ALLOCATOR_H_

#include <stdint.h>
#include <cstddef>
#include <cpuinfo.h>
#include <cassert>

template<class T>
class array_allocator {
 private:
        T	 		*_free_list;
        uint32_t 		_cursor;
        uint32_t 		_sz;
        
 public:
        void* operator new(std::size_t sz, int cpu);
        array_allocator(uint32_t sz, int cpu);
        T* get();
        T* get_records(uint32_t *max);
        void reset();
};

template<class T>
void* array_allocator<T>::operator new(std::size_t sz, int cpu)
{
        return alloc_mem(sz, cpu);
}

template<class T>
array_allocator<T>::array_allocator(uint32_t sz, int cpu)
{
        _free_list = (T*)alloc_mem(sizeof(T)*sz, cpu);
        memset(_free_list, 0x0, sizeof(T)*sz);
        _cursor = 0;
        _sz = sz;
}

template<class T>
T* array_allocator<T>::get()
{
        assert(_cursor < _sz - 1);
        _cursor += 1;
        return &_free_list[_cursor-1];
}

template<class T>
T* array_allocator<T>::get_records(uint32_t *max)
{
        *max = _cursor;
        return _free_list;
}

template<class T>
void array_allocator<T>::reset()
{
        memset(_free_list, 0x0, sizeof(T)*_sz);
        _cursor = 0;        
}

#endif 		// ARRAY_ALLOCATOR_H_
