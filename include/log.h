#ifndef           LOG_H_ 
#define           LOG_H_

#include <unordered_map>
#include <deque>
#include <vector>
#include <db.h>

enum log_status_t {
    ACTIVE,
    COMMITTED, 
};

struct LogRecord {
    void *value;
    uint64_t tid;
    log_status_t status; 
};

class Log {

  private:
    // in-memory precommitted objects map
    typedef std::deque<uint64_t>    tid_queue;  
    typedef std::unordered_map<big_key, tid_queue*>  precommit_map;  
    precommit_map precommittedObjects; 

    // log storage
    // TODO: We temporarily use in-memory struture. Will change this to persistent key-value storage
    typedef std::vector<LogRecord>  log_list;
    typedef std::unordered_map<big_key, log_list*> log_storage; 
    log_storage logRecords;
    
  public:
    Log() {}
    
    // Helpers for precommitted object map
    void append_log_records(uint32_t tableId, uint64_t key, uint64_t tid, void *value);
    void register_to_pom(uint32_t tableId, uint64_t key, uint64_t tid);
    uint64_t get_precommitted_tid(uint32_t tableId, uint64_t key);
};


#endif            /* LOG_H_ */
