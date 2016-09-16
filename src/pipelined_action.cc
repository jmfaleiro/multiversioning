#include <pipelined_action.h>

mcs_rw::mcs_rw_lock* pipelined::get_lock(void* record)
{
        return (mcs_rw::mcs_rw_lock*)record;
}
 
locking_key** pipelined::get_deplist(void *record)
{
        return (locking_key**)PIPELINED_DEP_PTR(record);
}

void* pipelined::get_record(void *record)
{
        uint64_t *temp;
        
        temp = (uint64_t*)PIPELINED_VALUE_PTR(record);
        return &temp[1];
}

uint32_t pipelined::action::get_num_actions()
{
        return _num_actions;
}

uint32_t pipelined::action::get_type()
{
        return _type;
}

locking_action** pipelined::action::get_actions()
{
        return _actions;
}
