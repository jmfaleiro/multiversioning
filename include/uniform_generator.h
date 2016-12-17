#ifndef         UNIFORM_GENERATOR_H_
#define         UNIFORM_GENERATOR_H_

#include <record_generator.h>

class UniformGenerator : public RecordGenerator {
 private:
  uint64_t numElems; 
  uint32_t partitionNum;
  uint32_t partitions;

 public:
  UniformGenerator(uint64_t numElems, uint32_t partitions = 0, uint32_t partitionNum = 0) {
    this->numElems = numElems;
    if (partitions > 0)
      this->partitions = partitions - 1;
    else
      this->partitions = partitions;
    this->partitionNum = partitionNum;

    std::cout << "numElems " << this->numElems << ", partitions " << this->partitions << ", partitionNum " << this->partitionNum << endl;
  }

  virtual ~UniformGenerator() {
  }
  
  virtual uint64_t GenNext() {
    if (partitions > 1) {
      uint32_t partitionElems = this->numElems / partitions;
      uint64_t next = (uint64_t)rand() % partitionElems + partitionNum * partitionElems;
      return next;
    }
    return (uint64_t)rand() % this->numElems;
  }
};

#endif          // UNIFORM_GENERATOR_H_
