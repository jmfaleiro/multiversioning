#include <split_action.h>
#include <util.h>

bool split_action::ready()
{       
        uint64_t deps;
        barrier();
        deps = num_partition_dependencies + num_intra_dependencies;
        barrier();
        return (deps == 0);
}

void split_action::set_lock_list(void* list_ptr)
{
        this->list_ptr = list_ptr;
}

void* split_action::get_lock_list()
{
        return this->list_ptr;
}

void split_action::add_partition_dependency()
{
        this->num_partition_dependencies += 1;
}

void split_action::remove_partition_dependency()
{
        this->num_partition_dependencies -= 1;
}
