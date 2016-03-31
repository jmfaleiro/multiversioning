#include <zipf_generator.h>
#include <cassert>
#include <algorithm>
#include <cpuinfo.h>

// Each thread computes the contribution of a specific range of elements to zeta
double ZipfGenerator::ZetaPartition(ZetaParams* zetaParams) {
  double sum = 0;
  for (uint64_t i = zetaParams->start; i <= zetaParams->end; ++i) {
    sum += 1.0 / pow((double)i, zetaParams->theta);
  }
  return sum;
}

double ZipfGenerator::GenZeta(uint64_t numElems, double theta)
{
  ZetaParams params;
  params.start = 1;
  params.end = numElems;
  params.theta = theta;
  return ZetaPartition(&params);
}

ZipfGenerator::ZipfGenerator(uint64_t numElems, double theta) {
        uint64_t i;
        //        std::random_device rd;
        //        std::mt19937 g(rd());

        this->theta = theta;
        this->numElems = numElems;
        this->zetan = GenZeta(numElems, theta);  
        this->perm = (uint64_t*)zmalloc(sizeof(uint64_t)*numElems);
        for (i = 0; i < numElems; ++i) 
                perm[i] = i;
        std::random_shuffle(perm, &perm[numElems]);
}

uint64_t ZipfGenerator::GenNext() {
        /*
        uint64_t temp = (100.0 * theta);
        uint64_t ret = rand() % 100;
        if (ret < temp) {
                return (uint64_t)rand() % 200;
        } else {
                while (true) {
                        ret = (uint64_t)rand() % numElems;
                        if (ret < 200)
                                continue;
                        else 
                                return ret;
                }
        }
        */


  double alpha = 1 / (1 - this->theta);
  double eta = (1 - pow(2.0 / this->numElems, 1 - this->theta));
  double u = (double)rand() / ((double)RAND_MAX);
  double uz = u * this->zetan;
  uint64_t index;
  if (uz < 1.0) {
          index = 0;
  }
  else if (uz < (1.0 + pow(0.5, this->theta))) {
          index = 1;
  }
  else {
    uint64_t temp = (uint64_t)(this->numElems*pow(eta*u - eta + 1, alpha));
    assert(temp > 0 && temp <= this->numElems);
    index = temp - 1;
  }
  //  return index;
  return perm[index];

}

ZipfGenerator::~ZipfGenerator()
{
}
      
