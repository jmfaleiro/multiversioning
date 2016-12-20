#ifndef           LOG_H_ 
#define           LOG_H_

#include <unordered_map>
#include <deque>
#include <vector>
#include <sstream>
#include <db.h>
#include <stdint.h>
extern "C" {
  #include "fuzzy_log.h"
}

enum tid_status_t {
    TID_ACTIVE = 1,
    TID_PRECOMMITTED,
    TID_COMMITTED,
};

enum log_status_t {
    LOG_ACTIVE,
    LOG_COMMITTED, 
};

class MultiKeyLogRecord {
  private:
    char* logbuffer;
  
    void *value;
    uint64_t tid;
    log_status_t status; 

  public:
    MultiKeyLogRecord() {
      logbuffer = (char *)malloc(size());
    }
    ~MultiKeyLogRecord() {
      delete logbuffer;
    }

    void append(void *value, uint64_t tid, log_status_t status);
    uint32_t size() {
      return sizeof(value) + sizeof(tid) + sizeof(status);
    } 
    char* ptr() {
      return logbuffer;
    }

};

class SingleKeyLogRecord {
  private:
    char* logbuffer;
    uint32_t num_writes;
    uint32_t index;

    struct LogRecord{
      uint32_t table_id;
      uint64_t key;
      uint64_t value;
    };

  public:
    SingleKeyLogRecord(uint32_t num_writes) {
      this->num_writes = num_writes;
      this->index = 0;
      logbuffer = (char *)malloc(sizeof(LogRecord)*num_writes);
    }
    ~SingleKeyLogRecord() {
      delete logbuffer;
    }

    void append(uint32_t tableId, uint64_t key, uint64_t value);
    uint32_t size() {
      return sizeof(LogRecord)*num_writes;
    } 
    char* ptr() {
      return logbuffer;
    }
};

class Log {

  private:
    // Fuzzylog server
    std::string logServerIp;

  public:
    Log();
    ~Log(){}
    
    struct DAGHandle *get_fuzzy_log_client(struct colors *log_color);
    void close_fuzzy_log_client(struct DAGHandle *dag);

    // Interface for single-key logging
    void append_singlekey_log_records(struct DAGHandle *dag, struct colors *log_color, SingleKeyLogRecord *logs);

    // Interface for multi-key logging 
    void append_multikey_log_records(uint32_t tableId, uint64_t key, std::stringstream &log_record);
    void commit_log_records(uint32_t tableId, uint64_t key, uint64_t tid);
};


#endif            /* LOG_H_ */
