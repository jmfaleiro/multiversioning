#ifndef 	SIMPLE_SPLIT_H_
#define 	SIMPLE_SPLIT_H_

#include <vector>
#include <split_action.h>

using namespace std;

/*
 * simple_split transactions operate on a single table. The table's records are 
 * uint64_t's. For each element in "records", a simple_split txn updates the 
 * corresponding uint64_t.
 */
class simple_split : public txn {
 private:
        vector<uint64_t> *records;

 public:
        simple_split(vector<uint64_t> records);
        virtual bool Run();
        
        /* Required for txn interface */
        virtual uint32_t num_rmws();
        virtual void get_rmws(struct big_key *array);
};

#endif 		// SIMPLE_SPLIT_H_	
