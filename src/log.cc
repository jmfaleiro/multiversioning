#include <mutex>
#include <unordered_map>
#include <deque>
#include <log.h>
#include <db.h>
#include <cassert>

std::mutex mtx_log;
std::mutex mtx_pom;

void Log::append_log_records(uint32_t tableId, uint64_t key, uint64_t tid, void *value) 
{
  mtx_log.lock();

  struct big_key k;
  log_list *log_records;

  k.table_id = tableId;
  k.key = key; 

  LogRecord log_record;
  log_record.value = value;
  log_record.tid = tid;
  log_record.status = ACTIVE;

  log_storage::iterator found = logRecords.find(k);
  if (found == logRecords.end()) {
    log_records = new log_list();
    log_records->push_back(log_record);
    logRecords[k] = log_records;
  } else {
    log_records = found->second;
    log_records->push_back(log_record);
  }

  mtx_log.unlock();
}

void Log::register_to_pom(uint32_t tableId, uint64_t key, uint64_t tid)
{
  mtx_pom.lock();

  struct big_key k;
  tid_queue *tids;

  k.table_id = tableId;
  k.key = key; 

  precommit_map::iterator found = precommittedObjects.find(k);
  if (found == precommittedObjects.end()) {
    tids = new tid_queue();
    tids->push_back(tid);
    precommittedObjects[k] = tids;
  } else {
    tids = found->second;
    tids->push_back(tid);
  }

  mtx_pom.unlock();
}

uint64_t Log::get_precommitted_tid(uint32_t tableId, uint64_t key)
{
  mtx_pom.lock();

  struct big_key k;
  tid_queue *tids;
  uint64_t latest_tid;

  k.table_id = tableId;
  k.key = key; 

  precommit_map::iterator found = precommittedObjects.find(k);
  if (found == precommittedObjects.end()) {
    mtx_pom.unlock();
    return 0;
  }

  tids = found->second;
  assert(tids->size() > 0);

  latest_tid = tids->back();

  mtx_pom.unlock();

  return latest_tid;
}
