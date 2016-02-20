#ifndef 	MESSAGE_BROKER_H_
#define 	MESSAGE_BROKER_H_

#include <runnable.hh>
#include <concurrent_queue.h>
#include <split_executor.h>

class message_broker : public Runnable {
 private:
        uint32_t 		_num_inputs;
        uint32_t 		_num_outputs;
        splt_comm_queue		**_input_queues;
        splt_comm_queue		**_output_queues;

 public:
        message_broker(splt_comm_queue **inputs, uint32_t num_inputs, 
                       splt_comm_queue **outputs, 
                       uint32_t num_outputs, int cpu_number);
        
        void proc_single_iter();
        void StartWorking();
        void Init();                
};

#endif 		// MESSAGE_BROKER_H_
