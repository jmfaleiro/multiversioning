#include <mutex>
#include <unordered_map>
#include <deque>
#include <sstream>
#include <log.h>
#include <db.h>
#include <cassert>
#include <occ_action.h>

Log::Log()
{
  logServerIp = "127.0.0.1:9990";
  logColor.numcolors = 1;
  logColor.mycolors = new ColorID[1]{100};
}

struct DAGHandle* Log::get_fuzzy_log_client() {
  return new_dag_handle_for_single_server(logServerIp.c_str(), &logColor);
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

void Log::append_singlekey_log_records(struct DAGHandle *dag, std::stringstream &log_stream) 
{
  std::string logstring = log_stream.str();
  //uint32_t log_size = logstring.size();
  //std::cout << "log record size: " << log_size << std::endl;
  append(dag, const_cast<char*>(logstring.data()), logstring.size(), &logColor, NULL);
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
