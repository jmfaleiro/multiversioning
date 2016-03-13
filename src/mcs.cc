#include <mcs.h>
#include <util.h>
#include <cassert>

mcs_mgr::mcs_mgr(uint32_t num_locks, int cpu) 
{
        mcs_struct *locks;
        uint32_t i;

        locks = (mcs_struct*)alloc_mem(sizeof(mcs_struct)*num_locks, cpu);
        memset(locks, 0x0, sizeof(mcs_struct)*num_locks);
        for (i = 0; i < num_locks; ++i) {
                locks[i]._is_held = 0;
                locks[i]._next = &locks[i+1];
        }
        locks[i]._next = NULL;
        _free_list = locks;
}

mcs_struct* mcs_mgr::get_struct()
{
        mcs_struct *ret;

        assert(_free_list != NULL);
        ret = _free_list;
        _free_list = (mcs_struct*)ret->_next;
        return ret;
}

void mcs_mgr::return_struct(mcs_struct *m_struct)
{
        assert(m_struct->_is_held == 1 && m_struct->_next == NULL);
        m_struct->_next = _free_list;
        _free_list = m_struct;
}

void mcs_mgr::lock(mcs_struct *lock)
{
        mcs_struct *old_tail;
        volatile uint64_t is_held;
        volatile mcs_struct **tail;
        
        tail = lock->_tail_ptr;
        lock->_next = NULL;
        lock->_is_held = 0;
        old_tail = (mcs_struct*)xchgq((volatile uint64_t*)tail, (uint64_t)lock);
        
        if (old_tail == NULL) {
                lock->_is_held = 1;
        } else {
                old_tail = 
                        (mcs_struct*)xchgq((volatile uint64_t*)&old_tail->_next,
                                           (uint64_t)lock);
                //                assert(old_tail == NULL);
                do {
                        barrier();
                        is_held = lock->_is_held;
                        barrier();
                } while (is_held == 0);
        }
}

void mcs_mgr::unlock(mcs_struct *lock)
{
        mcs_struct *tail_read, *next_lock;
        uint64_t old_val;
        volatile mcs_struct **tail;
        
        tail = lock->_tail_ptr;
        barrier();
        tail_read = (mcs_struct*)*tail;
        barrier();
        
        if (tail_read == lock) 
                if (cmp_and_swap((volatile uint64_t*)tail, (uint64_t)lock, 
                                 (uint64_t)NULL)) 
                        return;
        do {
                barrier();
                next_lock = (mcs_struct*)lock->_next;
                barrier();
        } while (next_lock == NULL);
        old_val = xchgq((volatile uint64_t*)&next_lock->_is_held, 1);
        assert(old_val == 0);
        lock->_next = NULL;
}
