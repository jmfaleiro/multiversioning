#include <message_broker.h>
#include <algorithm>

/*
static bool msg_cmp(const split_message& msg1, const split_message& msg2)
{
        return msg1.partition < msg2.partition;
}
*/

message_broker::message_broker(splt_comm_queue **inputs, uint32_t num_inputs, 
                               splt_comm_queue **outputs, 
                               uint32_t num_outputs, int cpu_number) 
        : Runnable(cpu_number)
{
        _input_queues = inputs;
        _num_inputs = num_inputs;
        _output_queues = outputs;
        _num_outputs = num_outputs;
}

void message_broker::proc_single_iter()
{
        uint32_t i, j, nelems;
        split_message msg;
        bool success;

        for (i = 0; i < _num_inputs; ++i) {
                nelems = _input_queues[i]->diff();
                for (j = 0; j < nelems; ++j) {
                        success = _input_queues[i]->Dequeue(&msg);
                        assert(success);
                        _output_queues[msg.partition]->EnqueueBlocking(msg);
                }                        
        }
        
        /*
        bool success;
        uint64_t num_elems[_num_inputs], total, i, msg_ptr, j;
        uint32_t partition;
        

        total = 0;
        for (i = 0; i < _num_inputs; ++i) {
                num_elems[i] = _input_queues[i]->diff();
                total += num_elems[i];
        }


        split_message msg[total];
        msg_ptr = 0;
        for (i = 0; i < _num_inputs; ++i) {
                for (j = 0; j < num_elems[i]; ++j) {
                        success = _input_queues[i]->Dequeue(&msg[msg_ptr]);
                        assert(success == true);
                        msg_ptr += 1;
                }
        }
        

        std::sort(msg, &msg[total], msg_cmp);
        for (i = 0; i < total; ++i) {
                partition = msg[i].partition;
                assert(_output_queues[partition] != NULL);
                _output_queues[partition]->EnqueueBlocking(msg[i]);
        }
        */
}

void message_broker::StartWorking()
{
        uint32_t i;

        while (true) 
                for (i = 0; i < _num_inputs; ++i) 
                        proc_single_iter();
}

void message_broker::Init()
{
}
