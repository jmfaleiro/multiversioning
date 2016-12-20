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

void Log::append_singlekey_log_records(struct DAGHandle *dag, struct colors *log_color, SingleKeyLogRecord *logs) 
{
  append(dag, logs->ptr(), logs->size(), log_color, NULL);
}

void Log::append_multikey_log_records(uint32_t tableId, uint64_t key, std::stringstream &log_record) 
{
  struct colors logColorPerObj;

  std::string logstring = log_record.str();
  // TODO: 
  (void)tableId;

  // make color
  logColorPerObj.numcolors = 1;
  logColorPerObj.mycolors = new ColorID[1]{(uint32_t)key};
  DAGHandle *dagHandle = new_dag_handle_for_single_server(logServerIp.c_str(), &logColorPerObj);

  append(dagHandle, const_cast<char*>(logstring.data()), logstring.size(), &logColorPerObj, NULL);
}

void Log::commit_log_records(uint32_t tableId, uint64_t key, uint64_t tid)
{
  // TODO:
  (void)tableId;
  (void)key;
  (void)tid;
}



void MultiKeyLogRecord::append(void *value, uint64_t tid, log_status_t status)
{
    memcpy(logbuffer, &value, sizeof(value));
    memcpy(logbuffer + sizeof(value), &tid, sizeof(tid));
    memcpy(logbuffer + sizeof(value) + sizeof(tid), &status, sizeof(status));
}

void SingleKeyLogRecord::append(uint32_t tableId, uint64_t key, uint64_t value)
{
    char* buf = logbuffer + sizeof(LogRecord)*index;
    memcpy(buf, &tableId, sizeof(tableId));
    memcpy(buf + sizeof(tableId), &key, sizeof(key));
    memcpy(buf + sizeof(tableId) + sizeof(key), &value, sizeof(value));
    
    this->index++;
}

