#include <gtest/gtest.h>
#include <setup_split.h>

uint32_t GLOBAL_RECORD_SIZE;
uint32_t NUM_CC_THREADS;
uint64_t recordSize;

uint32_t setup_split::num_split_tables = 0;
uint64_t* setup_split::split_table_sizes = NULL;

int main(int argc, char **argv)
{
        srand(time(NULL));
        testing::InitGoogleTest(&argc, argv);
        return RUN_ALL_TESTS();
}
