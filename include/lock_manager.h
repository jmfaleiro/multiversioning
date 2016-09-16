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
        bool LockRecord(locking_action *txn, struct locking_key *k);  
        void UnlockRecord(locking_action *txn, struct locking_key *k);
        void commit_write(locking_action *txn, struct locking_key *k);

public:
    LockManager(table_mgr *tbl_mgr);
    virtual bool Lock(locking_action *txn);
    virtual void Unlock(locking_action *txn);

    static bool SortCmp(const locking_key &key1, const locking_key &key2);
};

#endif // LOCK_MANAGER_HH_
