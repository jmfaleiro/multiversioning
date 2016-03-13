#include <shared_split_scheduler.h>

split_dep* dep_manager::cur_ptr()
{
        assert(_dep_index >= _dep_watermark);
        while (_dep_index - _dep_watermark == _num_deps)
                gc();
        return &_dep_array[_dep_index];
}

split_dep* dep_manager::get_dep()
{
        assert(_dep_index >= _dep_watermark);
        while (_dep_index - _dep_watermark == _num_deps) 
                gc();
        
        assert(_dep_index >= _dep_watermark && 
               _dep_index - _dep_watermark < _num_deps);

        split_dep *ret;
        ret = &_dep_array[_dep_index];
        _dep_index += 1;
        return ret;
}

void finish_epoch()
{
        assert(_range_index >= _range_watermark);
        while (_range_index - _range_watermark == _num_ranges)
                gc();

        assert(_range_index >= _range_watermark &&
               _range_index - _range_watermark < _num_ranges);
        
        _range_ptrs[_range_index] = _dep_index;
        _range_index += 1;
}

void dep_manager::gc()
{
        assert(_range_watermark == _last_collected);

        uint64_t cur_epoch;
        
        barrier();
        cur_epoch = *_low_watermark_ptr;
        barrier();
        
        if (cur_epoch > _last_collected) {
                _range_watermark = cur_epoch;
                _dep_watermark = _range_ptrs[_range_watermark];
                _last_collected = cur_epoch;
        }
}


void shared_split_scheduler::StartWorking()
{
        split_action_batch batch;
        
        while (true) {
                batch = config._input_queue->DequeueBlocking();
                schedule_batch(batch);
                
        }
}

void shared_split_scheduler::schedule_batch(split_action_batch batch)
{
        uint32_t i;
        
        for (i = 0; i < batch.num_actions; ++i) 
                schedule_action(actions[i]);
}

void shared_split_scheduler::schedule_action(split_action *action)
{
        uint32_t i, nreads, nwrites;
        
        action->_dependencies = _dep_allocator->cur_ptr();
        nreads = action->_readset.size();
        for (i = 0; i < nreads; ++i) 
                schedule_operation(action->_readset[i], action, SPLIT_READ);

        nwrites = action->_writeset.size();
        for (i = 0; i < nwrites; ++i)
                schedule_operation(action->_writeset[i], action, SPLIT_WRITE);
}

void shared_split_executor::schedule_operation(big_key &key, 
                                               split_action *action, 
                                               access_t type)
{
        split_record *entry;
        split_dep *prev, *cur;
        
        /* XXX get a hold of the record somehow */
        assert(record->key == key);        
        prev = entry->key_struct;
        assert(record->epoch != epoch || prev == NULL || prev->key == key);

        cur = _dep_allocator->get_dep();
        cur->_key = key;
        cur->_action = action;
        cur->_type = type;
        cur->_read_count = 1;
        cur->_read_dep = cur;
        
        if (prev == NULL || entry->epoch != epoch) {
                entry->epoch = epoch;
                cur->_dep = NULL;
                entry->key_struct = cur;
        } else if (cur->_type == SPLIT_WRITE) {
                cur->_dep = prev->_action;
                cur->_read_count = 0;
                entry->key_struct = cur;
                assert(cur->_dep != action);
        } else if (cur->_type == SPLIT_READ && prev->_type == SPLIT_READ) {
                cur->_dep = prev->_dep;
                prev->_read_count += 1;
                cur->_read_dep = prev;
                assert(cur->_dep != action);
        } else if (prev->_type == SPLIT_WRITE) {
                cur->_dep = prev->_action;
                cur->_read_count = 1;
                cur->_read_dep = cur;
                entry->key_struct = cur;
        } else {
                assert(false);
        }
}
