#include <eager_worker.h>

extern size_t *tpcc_record_sizes;


locking_worker::locking_worker(locking_worker_config config,
                               RecordBuffersConfig rb_conf) 
    : Runnable(config.cpu)
{
        this->config = config;
        m_queue_head = NULL;
        m_queue_tail = NULL;    
        m_num_elems = 0;
        m_num_done = 0;
        this->bufs = new(config.cpu) RecordBuffers(rb_conf);
        this->mgr = new(config.cpu) mcs_mgr(1000, config.cpu);
        this->insert_mgr = new(config.cpu) insert_buf_mgr(config.cpu, 11, 
                                                          tpcc_record_sizes);

#ifdef 	TPCC
        this->_warehouses = config.warehouses;
        this->_districts = config.district;
#endif
        
}

void locking_worker::Init()
{
}

void
locking_worker::StartWorking()
{
        WorkerFunction();
}

void locking_worker::Enqueue(locking_action *txn)
{
        if (m_queue_head == NULL) {
                assert(m_queue_tail == NULL);
                m_queue_head = txn;
                m_queue_tail = txn;
                txn->prev = NULL;
                txn->next = NULL;
        }else {
                m_queue_tail->next = txn;
                txn->next = NULL;
                txn->prev = m_queue_tail;
                m_queue_tail = txn;
        }
        m_num_elems += 1;
        assert((m_queue_head == NULL && m_queue_tail == NULL) ||
               (m_queue_head != NULL && m_queue_tail != NULL));
}

void locking_worker::RemoveQueue(locking_action *txn)
{
        locking_action *prev = txn->prev;
        locking_action *next = txn->next;

        if (m_queue_head == txn) {
                assert(txn->prev == NULL);
                m_queue_head = txn->next;
        } else {
                prev->next = next;
        }
    
        if (m_queue_tail == txn) {
                assert(txn->next == NULL);
                m_queue_tail = txn->prev;
        } else {
                next->prev = prev;
        }
    
        m_num_elems -= 1;
        assert(m_num_elems >= 0);
        assert((m_queue_head == NULL && m_queue_tail == NULL) ||
               (m_queue_head != NULL && m_queue_tail != NULL));
}

uint32_t
locking_worker::QueueCount(locking_action *iter)
{
        if (iter == NULL) 
                return 0;
        else
                return 1+QueueCount(iter->next);
}

void locking_worker::CheckReady()
{
        locking_action *iter;
        for (iter = m_queue_head; iter != NULL; iter = iter->next) {
                if (iter->num_dependencies == 0 && config.mgr->Lock(iter)) {
                                RemoveQueue(iter);
                                DoExec(iter);
                }
        }
}

void locking_worker::give_locks(__attribute__((unused)) locking_action *txn)
{
        //        uint32_t num_writes, num_reads, i;
        mcs_struct *lck;

        lck = mgr->get_struct();
        txn->lck = lck;
        /*
        num_writes = txn->writeset.size();
        for (i = 0; i < num_writes; ++i) 
                txn->writeset[i].lock_entry = lck;
        num_reads = txn->readset.size();
        for (i = 0; i < num_reads; ++i) 
                txn->readset[i].lock_entry = lck;
        */
}

void locking_worker::take_locks(__attribute__((unused)) locking_action *txn)
{
        //        assert(false);
        
        mgr->return_struct(txn->lck);
        /*
        uint32_t num_writes, num_reads;
        
        num_writes = txn->writeset.size();
        num_reads = txn->readset.size();
        if (num_writes > 0) {
                mgr->return_struct(txn->writeset[0].lock_entry);
        } else {
                assert(num_reads > 0);
                mgr->return_struct(txn->readset[0].lock_entry);
        }
        */
}

void locking_worker::TryExec(locking_action *txn)
{
        txn->tables = this->config.tbl_mgr;
        txn->insert_mgr = this->insert_mgr;
        txn->lock_mgr = config.mgr;
        if (config.mgr->Lock(txn)) {
                assert(txn->num_dependencies == 0);
                assert(txn->bufs == NULL);
                txn->bufs = this->bufs;
                txn->worker = this;
                txn->Run();
                config.mgr->Unlock(txn);                
                take_locks(txn);
                assert(txn->finished_execution);
        } else {
                m_num_done += 1;
                Enqueue(txn);
        }
}

void locking_worker::DoExec(locking_action *txn)
{
        assert(txn->num_dependencies == 0);
        assert(txn->bufs == NULL);
        txn->bufs = this->bufs;    
        txn->worker = this;
        txn->Run();
        config.mgr->Unlock(txn);
        take_locks(txn);
}

void
locking_worker::WorkerFunction()
{
        locking_action_batch batch;
        //        double results[1000];

        // Each iteration of this loop executes a batch of transactions
        while (true) {
                batch = config.inputQueue->DequeueBlocking();
                for (uint32_t i = 0; i < batch.batchSize; ++i) {
                        
                        // Ensure we haven't exceeded threshold of max deferred
                        // txns. If we have, exec pending txns so we get below
                        // the threshold.
                        if ((uint32_t)m_num_elems < config.maxPending) {
                                batch.batch[i]->worker = this;
                                give_locks(batch.batch[i]);
                                TryExec(batch.batch[i]);
                        } else { 
                                while (m_num_elems >= config.maxPending) 
                                        CheckReady();
                        }
                }
                
                // Finish deferred txns
                while (m_num_elems != 0) 
                        CheckReady();
        
                // Signal that this batch is done
                config.outputQueue->EnqueueBlocking(batch);
        }
 
}
