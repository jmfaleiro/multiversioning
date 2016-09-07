#ifndef LOCK_MANAGER_HH_
#define LOCK_MANAGER_HH_

#include <lock_manager_table.h>
//#include <locking_action.h>
#include <deque>
#include <pthread.h>

using namespace std;

class locking_action;

class LockManager {    

 private:
        LockManagerTable *table;
        uint64_t *tableSizes;
        bool LockRecord(locking_action *txn, struct locking_key *dep);  

public:
    LockManager(LockManagerConfig config);
    virtual bool Lock(locking_action *txn);
    virtual void Unlock(locking_action *txn);

#ifdef 	RUNTIME_PIPELINING
    virtual void ReleaseTable(locking_action *txn, uint32_t table_id);
#endif

    static bool SortCmp(const locking_key &key1, const locking_key &key2);
};

#endif // LOCK_MANAGER_HH_
