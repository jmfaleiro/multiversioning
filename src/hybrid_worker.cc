#include <hybrid_worker.h>

void* hybrid_worker::operator new(std::size_t sz, int cpu)
{
    return alloc_mem(sz, cpu);
}

hybrid_worker::hybrid_worker(int cpu, MVSchedulerConfig sched_conf, 
                             ExecutorConfig exec_conf)
    : Runnable(cpu)
{
    _scheduler = new(cpu) MVScheduler(sched_conf);
    _executor = new(cpu) Executor(exec_conf);
}

void hybrid_worker::Init()
{
}

void hybrid_worker::StartWorking()
{
    while (true) {
        _scheduler->single_iteration();
        _scheduler->wait_batch();
        _executor->single_iteration();
        _executor->wait_batch();
    }
}
