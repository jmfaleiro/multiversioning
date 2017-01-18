#ifndef 	HYBRID_WORKER_H_
#define 	HYBRID_WORKER_H_

#include <runnable.hh>
#include <preprocessor.h>
#include <executor.h>

class hybrid_worker : public Runnable {

 private:
    MVScheduler 	*_scheduler;
    Executor 		*_executor;
    
 protected:
    virtual void StartWorking();
    virtual void Init();

 public:
    void* operator new(std::size_t sz, int cpu);
    hybrid_worker(int cpu, MVSchedulerConfig sched_config, 
                  ExecutorConfig exec_config);
};

#endif 		// HYBRID_WORKER_H_
