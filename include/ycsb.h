#ifndef YCSB_H_
#define YCSB_H_

#include <db.h>
#include <vector>

#define YCSB_RECORD_SIZE 1000

using namespace std;

class ycsb_insert : public txn {
 private:
        uint64_t start;
        uint64_t end;

        void gen_rand(char *array);
        
 public:
        ycsb_insert(uint64_t start, uint64_t end);
        virtual bool Run();
        virtual uint32_t num_writes();
        virtual void get_writes(struct big_key *array);
};

class ycsb_readonly : public txn {
 private:
        volatile uint64_t accumulated;
        vector<uint64_t> reads;
 public:
        ycsb_readonly(vector<uint64_t> reads);
        virtual bool Run();
        virtual uint32_t num_reads();
        virtual void get_reads(struct big_key *array);
};

class ycsb_rmw : public txn {
 private:
        vector<uint64_t> reads;
        vector<uint64_t> writes;
        
 public:
        ycsb_rmw(vector<uint64_t> reads, vector<uint64_t> writes);
        virtual bool Run();
        virtual uint32_t num_reads();
        virtual uint32_t num_rmws();
        virtual void get_reads(struct big_key *array);
        virtual void get_rmws(struct big_key *array);
};

class ycsb_update : public txn {
 private:
        uint64_t _updates[10];
        vector<uint64_t> _writes;

 public:
        ycsb_update(vector<uint64_t> writes, uint64_t *updates);
        virtual bool Run();
        virtual uint32_t num_rmws();
        virtual void get_rmws(struct big_key *array);
};

class split_ycsb_read;
class split_ycsb_acc;
class split_ycsb_update;

class split_ycsb_read : public txn {
        friend class split_ycsb_acc;
        friend class split_ycsb_update;

 private:
        uint32_t 		*_accumulated;
        vector<uint64_t> 	_reads;

        void process_record(uint32_t *acc, uint64_t key);
 public:
        split_ycsb_read(uint32_t *acc_array, vector<uint64_t> reads);
        virtual bool Run();        
        uint32_t num_reads();
        void get_reads(big_key *array);
        
};

class split_ycsb_acc : public txn {
        friend class split_ycsb_update;
 private:
        uint32_t 			*_accumulated;
        vector<split_ycsb_read*> 	_read_txns;

 public:
        split_ycsb_acc(vector<split_ycsb_read*> read_txns);
        virtual bool Run();        
};

class split_ycsb_update : public txn {
 private:
        //        split_ycsb_acc 			*_accumulator;
        vector<uint64_t> 		_writes;
        //        vector<split_ycsb_read*> 	_reads;
        uint32_t 			*_accumulated;
        uint32_t 			_nreads;
        
        void get_values();
 public:
        split_ycsb_update(uint32_t *accumulated, uint32_t nreads, vector<uint64_t> writes);
        virtual bool Run();
        virtual uint32_t num_rmws();
        virtual void get_rmws(big_key *array);        
};

class ycsb_read_write : public txn {
 private:
        vector<uint64_t> 		_reads;
        vector<uint64_t> 		_writes;
        uint64_t			_accumulated[10];

 public:
        ycsb_read_write(vector<uint64_t> reads, vector<uint64_t> writes);
        virtual bool Run();
        virtual uint32_t num_reads();
        virtual void get_reads(big_key *array);
        virtual uint32_t num_rmws();
        virtual void get_rmws(big_key *array);
};

#endif // YCSB_H_
