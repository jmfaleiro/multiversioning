#ifndef           LOG_H_ 
#define           LOG_H_

#include <unordered_map>
#include <deque>
#include <vector>
#include <sstream>
#include <db.h>
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

struct LogRecord {
    void *value;
    uint64_t tid;
    log_status_t status; 
};

class Log {

  private:
    // Fuzzylog server
    struct colors logColor;
    std::string logServerIp;

    // Only used for multi-key logging
    // in-memory precommitted objects map
    typedef std::unordered_map<uint64_t, int> tid_status_map;  
    tid_status_map tidStatus; 
    std::mutex mtx_pom;

    typedef std::vector<std::function<void()> > callback_list;
    typedef std::unordered_map<uint64_t, callback_list*> callback_map;
    callback_map callbackMap;
    std::mutex mtx_callback;
    
  public:
    Log();
    ~Log();
    
    struct DAGHandle *get_fuzzy_log_client();
    void close_fuzzy_log_client(struct DAGHandle *dag);

    // Interface for single-key logging
    void append_singlekey_log_records(struct DAGHandle *dag, std::stringstream &log_stream);

    // Interface for multi-key logging 
    void append_multikey_log_records();
    void commit_log();

    // Only used for multi-key logging
    // Helpers for precommitted object map
    void update_tid_status(uint64_t tid, int status);
    int get_tid_status(uint64_t tid);
    void remove_tid_status(uint64_t tid);

    void add_callback(uint64_t tid, std::function<void()> callback);
    void execute_callbacks(uint64_t tid);
};


#endif            /* LOG_H_ */
