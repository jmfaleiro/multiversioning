#include <config.h>
#include <split_action.h>
#include <split_executor.h>
#include <sys/time.h>
#include <common.h>
#include <fstream>
#include <db.h>

#define LOCK_TABLE_SPACE (((uint64_t)1) << 29)	/* 512 M */

uint32_t num_split_tables = 0;
uint64_t *split_table_sizes = NULL;

/*
 * XXX Fix up this function to take different experiments into account.
 */
static void setup_table_info(split_config s_conf)
{
        assert(num_split_tables == 0 && split_table_sizes == NULL);
        num_split_tables = 1;
        split_table_sizes = 
                (uint64_t*)zmalloc(num_split_tables*sizeof(uint64_t));
        split_table_sizes[0] = s_conf.num_records;
}



static uint32_t get_num_lock_structs()
{
        return LOCK_TABLE_SPACE / sizeof(lock_struct);        
}

static uint32_t partition_hash(const big_key *key)
{
        return 0;
}

static void setup_input(split_config s_conf, workload_config w_conf)
{
        
}

/*
 * Setup communication queues between executor threads.
 */
static splt_comm_queue*** setup_comm_queues(split_config s_conf)
{
        splt_comm_queue ***ret;
        uint32_t i;
        
        ret = (splt_comm_queue***)zmalloc(sizeof(splt_comm_queue**)*
                                     s_conf.num_partitions);
        for (i = 0; i < s_conf.num_partitions; ++i) 
                ret[i] = setup_queues<split_action*>(s_conf.num_partitions, 
                                                     1024);
        return ret;
}

/*
 * Setup action input queues for executors.
 */
static splt_inpt_queue** setup_input_queues(split_config s_conf)
{
        splt_inpt_queue **ret;
        ret = setup_queues<split_action_batch>(s_conf.num_partitions, 1024);
        return ret;
}

/*
 * Setup lock table config.
 */
static struct lock_table_config setup_lock_table_config(uint32_t cpu, 
                                                        split_config s_conf)
{

        uint32_t num_lock_structs;
        num_lock_structs = get_num_lock_structs();
        struct lock_table_config ret = {
                cpu,
                num_split_tables,
                split_table_sizes,
                num_lock_structs,
        };
        return ret;
}

static struct split_executor_config setup_exec_config(uint32_t cpu, 
                                                      uint32_t num_partitions, 
                                                      uint32_t partition_id,
                                                      splt_comm_queue **ready_queues,
                                                      splt_inpt_queue *input_queue,
                                                      split_config s_conf)
{
        struct split_executor_config exec_conf;
        struct lock_table_config lck_conf;

        lck_conf = setup_lock_table_config(cpu, s_conf);
        exec_conf = {
                cpu,
                num_partitions,
                partition_id,
                ready_queues,
                input_queue,
                lck_conf,
        };
        return exec_conf;
}

/*
 * Setup executor threads.
 */
static split_executor** setup_threads(split_config s_conf, 
                                      splt_inpt_queue **in_queues)
{
        split_executor **ret;
        splt_comm_queue ***comm_queues;
        split_executor_config conf;
        uint32_t i;

        ret = (split_executor**)
                zmalloc(sizeof(split_executor*)*s_conf.num_partitions);
        comm_queues = setup_comm_queues(s_conf);
        for (i = 0; i < s_conf.num_partitions; ++i) {
                conf = setup_exec_config(i, s_conf.num_partitions, i, 
                                        comm_queues[i], 
                                        in_queues[i],
                                        s_conf);
                ret[i] = new(i) split_executor(conf);
        }
        return ret;
}

static void do_experiment()
{

}

void split_experiment(split_config s_conf, workload_config w_confx)
{
        splt_inpt_queue **input_queues;

        input_queues = setup_input_queues(s_conf);
        //        setup_input();
        setup_threads(s_conf, input_queues);
        //        do_experiment();
}
