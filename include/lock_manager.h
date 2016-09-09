#ifndef LOCK_MANAGER_HH_
#define LOCK_MANAGER_HH_

#include <deque>
#include <pthread.h>
#include <table_mgr.h>
#include <locking_action.h>

using namespace std;

class LockManager {    

 private:
        table_mgr 	*_tbl_mgr;
        bool LockRecord(struct locking_key *k);  
        void UnlockRecord(struct locking_key *k);

public:
    LockManager(table_mgr *tbl_mgr);
    virtual bool Lock(locking_action *txn);
    virtual void Unlock(locking_action *txn);

#ifdef 	RUNTIME_PIPELINING
    virtual void ReleaseTable(locking_action *txn, uint32_t table_id);
#endif

    static bool SortCmp(const locking_key &key1, const locking_key &key2);
};

#endif // LOCK_MANAGER_HH_
