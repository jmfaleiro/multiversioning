#include <mutex>
#include <unordered_map>
#include <deque>
#include <sstream>
#include <log.h>
#include <db.h>
#include <cassert>
#include <occ_action.h>
#include <mutex>

std::mutex mtx;

Log::Log()
{
  // FIXME: remove hardcoded IP 
  logServerIp = "52.15.156.76:9990";
}

struct DAGHandle* Log::get_fuzzy_log_client(struct colors *log_color) {
  mtx.lock();

  DAGHandle *dagHandle = new_dag_handle_for_single_server(logServerIp.c_str(), log_color);
  std::cout << "New DAGHandle " << dagHandle << std::endl;

  mtx.unlock();
  return dagHandle;
}

void Log::close_fuzzy_log_client(struct DAGHandle *dag) {
  close_dag_handle(dag);
}

Log::~Log()
{
  // cleanup callback map
  callback_list *callbacks;
  for (callback_map::iterator it = callbackMap.begin();
        it != callbackMap.end(); ++it) {
    callbacks = it->second;
    if (callbacks != NULL)
      delete callbacks;
  }
}

void Log::append_singlekey_log_records(struct DAGHandle *dag, struct colors *log_color, SingleKeyLogRecord *logs) 
{
  std::cout << "log record size: " << logs->size() << std::endl;
  append(dag, logs->ptr(), logs->size(), log_color, NULL);
}

void Log::append_multikey_log_records() 
{
  // TODO: Will be used in two-phase commit protocol
}

void Log::commit_log()
{
  // TODO: Will be used in two-phase commit protocol
}

void Log::update_tid_status(uint64_t tid, int status)
{
  mtx_pom.lock();
  tidStatus[tid] = status;
  mtx_pom.unlock();
}

int Log::get_tid_status(uint64_t tid)
{
  int status;
  mtx_pom.lock();
  tid_status_map::iterator found = tidStatus.find(tid);
  if (found == tidStatus.end()) {
    mtx_pom.unlock();
    return 0;
  }
  status = found->second;
  mtx_pom.unlock();

  return status;
}

void Log::remove_tid_status(uint64_t tid)
{
  mtx_pom.lock();

  tidStatus.erase(tid);

  mtx_pom.unlock(); 
}

void Log::add_callback(uint64_t tid, std::function<void()> callback)
{
  mtx_callback.lock();  
 
  callback_list *callbacks;

  callback_map::iterator found = callbackMap.find(GET_TIMESTAMP(tid));
  if (found == callbackMap.end()) {
    callbacks = new callback_list();
    callbacks->push_back(callback);
    callbackMap[GET_TIMESTAMP(tid)] = callbacks;
  } else {
    callbacks = found->second;
    callbacks->push_back(callback);
  }

  mtx_callback.unlock();  
}

void Log::execute_callbacks(uint64_t tid)
{
  mtx_callback.lock();  

  callback_list *callbacks;

  callback_map::iterator found = callbackMap.find(GET_TIMESTAMP(tid));
  if (found != callbackMap.end()) {
    callbacks = found->second;
    assert(callbacks != NULL);

    for (callback_list::iterator it = callbacks->begin();
          it != callbacks->end(); ++it) {
      (*it)();
    }
    callbacks->clear();
  }

  mtx_callback.unlock();  
}

void SingleKeyLogRecord::append(uint32_t tableId, uint64_t key, uint64_t value)
{
    char* buf = logbuffer + sizeof(LogRecord)*index;
    memcpy(buf, &tableId, sizeof(tableId));
    memcpy(buf + sizeof(tableId), &key, sizeof(key));
    memcpy(buf + sizeof(tableId) + sizeof(key), &value, sizeof(value));
    
    this->index++;
}

