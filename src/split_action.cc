#include <split_action.h>
#include <util.h>

txn_graph* split_txn::convert_to_graph()
{
        throw unimplemented_exception(0);
}

split_action::split_action(txn *t, uint32_t partition_id) : translator(t)
{
        dependents = NULL;
        num_dependents = 0;
        num_partition_dependencies = 0;
        list_ptr = NULL;
        num_intra_dependencies = 0;
        this->partition_id = partition_id;
        exec_list = NULL;        
}

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

uint32_t split_action::get_partition_id()
{
        return this->partition_id;
}

void split_action::set_rvp_wakeups(rendezvous_point **rvps, uint32_t count)
{
        this->rvps = rvps;
        this->rvp_count = count;
}

void split_action::set_rvp(rendezvous_point *rvp)
{
        if (rvp == NULL) {
                this->next = NULL;
        } else {
                this->next = rvp->to_run;
                rvp->to_run = this;
        }
}

/* XXX Incomplete */
void* split_action::write_ref(__attribute__((unused)) uint64_t key, 
                              __attribute__((unused)) uint32_t table_id)
{
        return NULL;
}

/* XXX Incomplete */
void* split_action::read(__attribute__((unused)) uint64_t key, 
                         __attribute__((unused)) uint32_t table_id)
{
        return NULL;
}

/* XXX Incomplete */
int split_action::rand()
{
        return 0;
}

/* XXX Incomplete */
bool split_action::run()
{
        return false;
}

/* XXX Incomplete */
void split_action::release_multi_partition()
{
}

