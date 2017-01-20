#!/usr/bin/python

import os
import sys
import clean

fmt_multi = "numactl --interleave=all build/db --cc_type 0 --num_cc_threads {0} --num_txns {1} --epoch_size 10000 --num_records {2} --num_worker_threads {3} --txn_size 10 --experiment {4} --record_size {7} --distribution {5} --theta {6} --read_pct {8} --read_txn_size 10000"

def gen_range(low, high, diff):
    ret = []
    while low <= high:
        ret.append(low)
        low += diff
    return ret


def scalability():
    outdir = "results/scalability"
    filename = "throughput.txt"
    ccThreads = 20
    txns = 3000000
    records = 1000000
    low_threads = 4
    high_threads = 40
    expt = 1
    distribution = 0
    theta = 0
    rec_size = 1000
    pct = 0
    
    for i in range(0, 10):
        mv_expt(outdir, filename, ccThreads, txns, records, low_threads, high_threads, expt, distribution, theta, rec_size, pct)

def mv_expt(outdir, filename, ccThreads, txns, records, lowThreads, highThreads, expt, distribution, theta, rec_size, pct, only_worker=False):
    outfile = os.path.join(outdir, filename)
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
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

def main():
    scalability()

if __name__ == "__main__":
    main()
