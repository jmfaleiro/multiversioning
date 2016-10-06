#!/usr/bin/python

import os
import sys
import os.path
import clean

fmt_split = "numactl --interleave=all build/db --cc_type 4 --num_partitions {0} --num_txns {1} --num_records {2} --txn_size {3} --experiment {4} --record_size {5} --distribution {6} --theta {7} --read_pct {9} --read_txn_size {10} --num_outstanding 100 --epoch_size 50000 --abort_pos {8} --num_warehouses {11}"

fmt_locking = "numactl --interleave=all build/db --cc_type 1  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size {8} --experiment {3} --record_size {6} --distribution {4} --theta {5} --read_pct {7} --read_txn_size 50 --num_warehouses {9} {10}"

fmt_pipelined = "numactl --interleave=all build/db --cc_type 5  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size {8} --experiment {3} --record_size {6} --distribution {4} --theta {5} --read_pct {7} --read_txn_size 50 --num_warehouses {9} {10}"

fmt_multi = "build/db --cc_type 0 --num_cc_threads {0} --num_txns {1} --epoch_size 10000 --num_records {2} --num_worker_threads {3} --txn_size 10 --experiment {4} --record_size {7} --distribution {5} --theta {6} --read_pct {8} --read_txn_size 10000"

fmt_rc = "numactl --interleave=all build/db --cc_type 2 --rc  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size {8} --experiment {3} --record_size {6} --distribution {4} --theta {5} --occ_epoch 8000000 --read_pct {7} --read_txn_size 50 --num_warehouses {9} {10}"


fmt_occ = "numactl --interleave=all build/db --cc_type 2  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size {8} --experiment {3} --record_size {6} --distribution {4} --theta {5} --occ_epoch 8000000 --read_pct {7} --read_txn_size 50 --num_warehouses {9} {10}"

fmt_hek = "build/db --cc_type 3  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size {6} --distribution {4} --theta {5} --occ_epoch 8000000 --read_pct {7} --read_txn_size 10000"

fmt_si = "build/si --cc_type 3  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size {6} --distribution {4} --theta {5} --occ_epoch 8000000 --read_pct {7} --read_txn_size 5"

fmt_multi_cc = "build/db --cc_type 0 --num_cc_threads {0} --num_txns {1} --epoch_size 10000 --num_records {2} --num_worker_threads {3} --txn_size {8} --experiment {4} --record_size {7} --distribution {5} --theta {6} --read_pct 0 --read_txn_size 10"


def main():
    ycsb()
    
def tpcc():
    fixed_dir = "results/tpcc/fixed_10"
    vary_dir = "results/tpcc/vary_wh"
    whs = [4, 8, 12, 16, 20, 24, 28, 32, 36, 40]
    ntxns = 3000000
    for w in whs:
        split_expt(vary_dir, "split.txt", w, w, ntxns, 1000000, 6, 1, 0.0, 1000, 20, 0, 0, 20, w)
#         occ_expt(vary_dir, "occ.txt", w, w, ntxns, 1000000, 6, 1, 0.0, 1000, 0, 20, w, True)
#         rc_expt(vary_dir, "rc.txt", w, w, ntxns, 1000000, 6, 1, 0.0, 1000, 0, 20, w, True)
# 
#         locking_expt(vary_dir, "locking.txt", w, w, ntxns, 1000000, 6, 1, 0.0, 1000, 0, 20, w, True)
#         pipelined_expt(vary_dir, "pipelined.txt", w, w, ntxns, 1000000, 6, 1, 0.0, 1000, 0, 20, w, True)


#         locking_expt(fixed_dir, "locking.txt", w, w, ntxns, 1000000, 6, 1, 0.0, 1000, 0, 20, 10, False)
#         pipelined_expt(fixed_dir, "pipelined.txt", w, w, ntxns, 1000000, 6, 1, 0.0, 1000, 0, 20, 10, False)
#         occ_expt(fixed_dir, "occ.txt", w, w, ntxns, 1000000, 6, 1, 0.0, 1000, 0, 20, 10, False)
#         rc_expt(fixed_dir, "rc.txt", w, w, ntxns, 1000000, 6, 1, 0.0, 1000, 0, 20, 10, False)
        split_expt(fixed_dir, "split.txt", w, w, ntxns, 1000000, 6, 1, 0.0, 1000, 20, 0, 0, 20, 10)

def ycsb():
    high_dir = "results/ycsb/update/high/"
    low_dir = "results/ycsb/update/low/"
    vary_dir = "results/ycsb/update/vary/"
    zipf_vals = [0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,0.0]
    ntxns = 3000000
    
    for i in range(0, 10):
        occ_expt(low_dir, "occ.txt", 4, 40, ntxns, 1000000, 4, 1, 0.0, 1000, 0, 20, 0, False)
        rc_expt(low_dir, "rc.txt", 4, 40, ntxns, 1000000, 4, 1, 0.0, 1000, 0, 20, 0, False)
        split_expt(low_dir, "split.txt", 4, 40, ntxns, 1000000, 4, 1, 0.0, 1000, 20, 0, 0, 20, 10)
        locking_expt(low_dir, "locking.txt", 4, 40, ntxns, 1000000, 4, 1, 0.0, 1000, 0, 20, 0, False)

        locking_expt(high_dir, "locking.txt", 4, 40, ntxns, 1000000, 4, 1, 0.9, 1000, 0, 20, 0, False)
        occ_expt(high_dir, "occ.txt", 4, 40, ntxns, 1000000, 4, 1, 0.9, 1000, 0, 20, 0, False)
        rc_expt(high_dir, "rc.txt", 4, 40, ntxns, 1000000, 4, 1, 0.9, 1000, 0, 20, 0, False)
        split_expt(high_dir, "split.txt", 4, 40, ntxns, 1000000, 4, 1, 0.9, 1000, 20, 0, 0, 20, 10)

    
        
def new_contention():
    result_dir = "results/ycsb/read_write/vary/"
    zipf_vals = [0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,0.0]
    
    ntxns = 3000000
    for z in zipf_vals:
        locking_expt(result_dir, "locking.txt", 40, 40, ntxns, 1000000, 5, 1, z, 1000, 0, 20)
        clean.theta_fn("locking", os.path.join(result_dir, "locking.txt"), os.path.join(result_dir, "locking_out.txt"))
        occ_expt(result_dir, "occ.txt", 40, 40, ntxns, 1000000, 5, 1, z, 1000, 0, 20)
        clean.theta_fn("locking", os.path.join(result_dir, "occ.txt"), os.path.join(result_dir, "occ_out.txt"))
        rc_expt(result_dir, "rc.txt", 40, 40, ntxns, 1000000, 5, 1, z, 1000, 0, 20)
        clean.theta_fn("locking", os.path.join(result_dir, "rc.txt"), os.path.join(result_dir, "rc_out.txt"))
        split_expt(result_dir, "split.txt", 40, 40, ntxns, 1000000, 5, 1, z, 1000, 20, 0, 0, 20)
        clean.theta_fn("locking", os.path.join(result_dir, "split.txt"), os.path.join(result_dir, "split_out.txt"))

        
def test_locking():
    reads_dir = "results/ycsb/read_write/vary"
    high_dir = "results/ycsb/aborts/high/"
    low_dir = "results/ycsb/aborts/low/"

    ntxns = 3000000
    for p in [0,2,4,6,8,10,12,14,16,18,20]:
        split_expt(low_dir, "split_new.txt", 40, 40, ntxns, 1000000, 4, 1, 0.0, 1000, 20, p, 0, 20, 0)
        clean.abort_fn(os.path.join(low_dir, "split_new.txt"), os.path.join(low_dir, "split_new_out.txt"))
        split_expt(high_dir, "split_new.txt", 40, 40, ntxns, 1000000, 4, 1, 0.9, 1000, 20, p, 0, 20, 0)
        clean.abort_fn(os.path.join(high_dir, "split_new.txt"), os.path.join(high_dir, "split_new_out.txt"))
        
    for t in [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]:
        split_expt(reads_dir, "split_new.txt", 40, 40, ntxns, 1000000, 4, 1, t, 1000, 20, 0, 0, 20, 0)
        clean.theta_fn("locking", os.path.join(reads_dir, "split_new.txt"), os.path.join(reads_dir, "split_new_out.txt"))

def print_cc():
    clean.cc_fn("results.txt", "cc_out.txt")

def single_cc_test(txn_size):
    os.system("rm results.txt")
    thread_range = [4,8,12,16,20]
    for t in thread_range:
        if t < 10:
            num_txns = 200000
        else:
            num_txns = 1000000
        cmd = fmt_multi_cc.format(str(t), str(num_txns), str(1000000), str(1),
                                  str(0), str(0), str(0.0), str(1000), str(txn_size))
        os.system(cmd)
    os.system("cat results.txt >>  cc_" + str(txn_size) + ".txt")


def test_cc():
    os.system("rm results.txt")
    for i in range(0, 10):
        single_cc_test(10)
        single_cc_test(5)
        single_cc_test(1)
    
        

def gen_range(low, high, diff):
    ret = []
    while low <= high:
        ret.append(low)
        low += diff
    return ret


def mv_expt_theta(outdir, filename, ccThreads, txns, records, threads, expt, distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p " + outdir)
    if not os.path.exists(outdep):
        os.system("rm results.txt")

        cmd = fmt_multi.format(str(ccThreads), str(txns), str(records), str(threads), str(expt), str(distribution), str(theta), str(rec_size))
        os.system(cmd)
        os.system("cat results.txt >>" + outfile)
        clean.theta_fn("mv", outfile, temp)
        saved_dir = os.getcwd()
        os.chdir(outdir)
        if expt == 0:
            os.system("gnuplot plot.plt")
        else:
            os.system("gnuplot plot1.plt")
        os.chdir(saved_dir)

def mv_expt_theta(outdir, filename, ccThreads, txns, records, threads, expt, distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    if not os.path.exists(outdep):
        os.system("rm results.txt")

        cmd = fmt_multi.format(str(ccThreads), str(txns), str(records), str(threads), str(expt), str(distribution), str(theta), str(rec_size))
        os.system(cmd)
        os.system("cat results.txt >>" + outfile)
        clean.theta_fn("mv", outfile, temp)
        saved_dir = os.getcwd()
        os.chdir(outdir)
        if expt == 0:
            os.system("gnuplot plot.plt")
        else:
            os.system("gnuplot plot1.plt")
        os.chdir(saved_dir)


def mv_expt_records(outdir, filename, ccThreads, txns, records, threads, expt, distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    if not os.path.exists(outdep):
        os.system("rm results.txt")

        cmd = fmt_multi.format(str(ccThreads), str(txns), str(records), str(threads), str(expt), str(distribution), str(theta), str(rec_size))
        
        subprocess.call(cmd)

        os.system("cat results.txt >>" + outfile)
        clean.records_fn(outfile, temp)
        saved_dir = os.getcwd()
        os.chdir(outdir)
        os.system("gnuplot plot.plt")
        os.chdir(saved_dir)


def locking_expt_records(outdir, filename, threads, txns, records, expt, distribution, theta, rec_size, num_warehouses):
    outfile = os.path.join(outdir, filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):
        os.system("rm locking.txt")

        cmd = fmt_locking.format(str(threads), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(num_warehouses))
            
        os.system(cmd)
        os.system("cat locking.txt >>" + outfile)
        clean.records_fn(outfile, temp)
        saved_dir = os.getcwd()
        os.chdir(outdir)
        os.system("gnuplot plot.plt")
        os.chdir(saved_dir)


def mv_expt_single(outdir, filename, ccThreads, txns, records, workers, expt, 
                   distribution, theta, rec_size, only_worker=False):
    outfile = os.path.join(outdir, filename)
    os.system("mkdir -p " + outdir)
    os.system("rm results.txt")
    cmd = fmt_multi.format(str(ccThreads), str(txns), str(records),
                           str(workers), str(expt), str(distribution),
                           str(theta), str(rec_size), str(0))
    os.system(cmd)
    os.system("cat results.txt >>" + outfile)
    saved_dir = os.getcwd()
    os.chdir(outdir)
    os.chdir(saved_dir)

def mv_single(outdir, filename, ccThreads, txns, records, worker_threads, expt,
              distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    os.system("mkdir -p " + outdir)
    os.system("rm results.txt")
    cmd = fmt_multi.format(str(ccThreads), str(txns), str(records),
                           str(worker_threads), str(expt), str(distribution),
                           str(theta), str(rec_size))
    os.system(cmd)
    os.system("cat results.txt >>" + outfile)
    saved_dir = os.getcwd()
    os.chdir(outdir)
    os.chdir(saved_dir)
    
        
def mv_expt(outdir, filename, ccThreads, txns, records, lowThreads, highThreads, expt, distribution, theta, rec_size, pct, only_worker=False):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)

        for i in val_range:
            os.system("rm results.txt")
            cmd = fmt_multi.format(str(ccThreads), str(txns), str(records), str(i), str(expt), str(distribution), str(theta), str(rec_size), str(pct))
            os.system(cmd)
            os.system("cat results.txt >>" + outfile)
            clean.clean_fn("mv", outfile, temp, only_worker)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)



def split_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size, txn_size, abort_pos, read_pct, read_txn_sz, num_warehouses):
    outfile = os.path.join(outdir, filename)
    
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)
        for i in val_range:
            os.system("rm split.txt")
            cmd = fmt_split.format(str(i), str(txns), str(records), str(txn_size), str(expt), str(rec_size), str(distribution), str(theta), str(abort_pos), str(read_pct), str(read_txn_sz), str(num_warehouses))
            os.system(cmd)
            os.system("cat split.txt >>" + outfile)
            clean.clean_fn("locking", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)


def pipelined_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size, read_pct, txn_size, num_warehouses, partitioned):
    outfile = os.path.join(outdir, filename)
    
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)
        for i in val_range:
            os.system("rm pipelined.txt")
            if partitioned:
                cmd = fmt_pipelined.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct), str(txn_size), str(num_warehouses), "--partitioned")
            else:
                cmd = fmt_pipelined.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct), str(txn_size), str(num_warehouses), "")
            os.system(cmd)
            os.system("cat locking.txt >>" + outfile)
            clean.clean_fn("locking", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)
    
def locking_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size, read_pct, txn_size, num_warehouses, partitioned):
    outfile = os.path.join(outdir, filename)
    
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)
        for i in val_range:
            os.system("rm locking.txt")
            if partitioned:
                cmd = fmt_locking.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct), str(txn_size), str(num_warehouses), "--partitioned")
            else:
                cmd = fmt_locking.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct), str(txn_size), str(num_warehouses), "")
            os.system(cmd)
            os.system("cat locking.txt >>" + outfile)
            clean.clean_fn("locking", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)



def occ_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size, read_pct, txn_size, num_warehouses, partitioned):
    outfile = os.path.join(outdir, filename)
    
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    
    os.system("mkdir -p " + outdir)
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)
        for i in val_range:
            os.system("rm occ.txt")
            if partitioned:                
                cmd = fmt_occ.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct), str(txn_size), str(num_warehouses), "--partitioned")
            else:
                cmd = fmt_occ.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct), str(txn_size), str(num_warehouses), "")
            os.system(cmd)
            os.system("cat occ.txt >>" + outfile)
            clean.clean_fn("occ", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)

def rc_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size, read_pct, txn_size, num_warehouses, partitioned):
    outfile = os.path.join(outdir, filename)
    
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    
    os.system("mkdir -p " + outdir)
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)
        for i in val_range:
            os.system("rm rc.txt")
            if partitioned:
                cmd = fmt_rc.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct), str(txn_size), str(num_warehouses), "--partitioned")
            else:
                cmd = fmt_rc.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct), str(txn_size), str(num_warehouses), "")
            os.system(cmd)
            os.system("cat rc.txt >>" + outfile)
            clean.clean_fn("occ", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)

if __name__ == "__main__":
    main()
