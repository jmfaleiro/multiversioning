#ifndef BATCH_ACTION_H_
#define BATCH_ACTION_H_

#include <db.h>

#include <stdint.h>
#include <unordered_set>

class BatchAction : public translator {
  public:
    typedef uint64_t RecKey;
    typedef std::unordered_set<RecKey> RecSet;
    BatchAction(txn* t): translator(t) {};

    virtual void add_read_key(RecKey rk) = 0;
    virtual void add_write_key(RecKey rk) = 0;
    
    virtual uint64_t get_readset_size() const = 0;
    virtual uint64_t get_writeset_size() const = 0;
    virtual RecSet* get_readset_handle() = 0;
    virtual RecSet* get_writeset_handle() = 0;

    virtual bool operator<(const BatchAction& ba2) const = 0;
};

#endif //BATCH_ACTION_H_
