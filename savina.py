#!/usr/bin/env python3

import argparse
import subprocess
import sys
import os
import numpy as np
import pandas as pd
from time import time

############################## CONFIGURATION ##############################

# Types of garbage collectors to use
gc_types = ["nogc", "wrc", "crgc-onblock", "crgc-wave"]

# Benchmarks to run in the "quick" evaluation.
# Benchmarks that take more than a second to run on average are commented out.
quick_benchmarks = [
    #### Microbenchmarks

    #"big.BigAkkaActorBenchmark",
    #"chameneos.ChameneosAkkaActorBenchmark",
    #"count.CountingAkkaGCActorBenchmark",
    "fib.FibonacciAkkaGCActorBenchmark",
    #"fjcreate.ForkJoinAkkaActorBenchmark",
    "fjthrput.ThroughputAkkaActorBenchmark",
    #"pingpong.PingPongAkkaActorBenchmark",
    "threadring.ThreadRingAkkaActorBenchmark",

    ### Concurrent benchmarks

    "banking.BankingAkkaManualStashActorBenchmark",
    "bndbuffer.ProdConsAkkaActorBenchmark",
    "cigsmok.CigaretteSmokerAkkaActorBenchmark",
    "concdict.DictionaryAkkaActorBenchmark",
    #"concsll.SortedListAkkaActorBenchmark",
    "logmap.LogisticMapAkkaManualStashActorBenchmark",
    #"philosopher.PhilosopherAkkaActorBenchmark",

    ### Parallel benchmarks

    "apsp.ApspAkkaGCActorBenchmark",
    #"astar.GuidedSearchAkkaGCActorBenchmark",
    "bitonicsort.BitonicSortAkkaActorBenchmark",
    "facloc.FacilityLocationAkkaActorBenchmark",
    "nqueenk.NQueensAkkaGCActorBenchmark",
    "piprecision.PiPrecisionAkkaActorBenchmark",
    "quicksort.QuickSortAkkaGCActorBenchmark",
    #"radixsort.RadixSortAkkaGCActorBenchmark",
    "recmatmul.MatMulAkkaGCActorBenchmark",
    "sieve.SieveAkkaActorBenchmark",
    "trapezoid.TrapezoidalAkkaActorBenchmark",
    "uct.UctAkkaActorBenchmark",
]

# Mappings from benchmarks to their names
benchmarks = {
    "apsp.ApspAkkaGCActorBenchmark": "All-Pairs Shortest Path",
    "astar.GuidedSearchAkkaGCActorBenchmark": "A-Star Search",
    "banking.BankingAkkaManualStashActorBenchmark": "Bank Transaction",
    #"barber.SleepingBarberAkkaActorBenchmark": "Sleeping Barber",              # Skipped due to unstable performance
    "big.BigAkkaActorBenchmark": "Big",
    "bitonicsort.BitonicSortAkkaActorBenchmark": "Bitonic Sort",
    "bndbuffer.ProdConsAkkaActorBenchmark": "Producer-Consumer",
    "chameneos.ChameneosAkkaActorBenchmark": "Chameneos",
    "cigsmok.CigaretteSmokerAkkaActorBenchmark": "Cigarette Smokers",
    "concdict.DictionaryAkkaActorBenchmark": "Concurrent Dictionary",
    "concsll.SortedListAkkaActorBenchmark": "Concurrent Sorted Linked List",
    "count.CountingAkkaGCActorBenchmark": "Counting Actor",
    "facloc.FacilityLocationAkkaActorBenchmark": "Online Facility Location",
    "fib.FibonacciAkkaGCActorBenchmark": "Recursive Fibonacci Tree",
    #"filterbank.FilterBankAkkaActorBenchmark": "Filter Bank",                 # Skipped due to deadlocks
    "fjcreate.ForkJoinAkkaActorBenchmark": "Fork Join (Actor Creation)",
    "fjthrput.ThroughputAkkaActorBenchmark": "Fork Join (Throughput)",
    "logmap.LogisticMapAkkaManualStashActorBenchmark": "Logistic Map Series",
    "nqueenk.NQueensAkkaGCActorBenchmark": "N-Queens",
    "philosopher.PhilosopherAkkaActorBenchmark": "Dining Philosophers",
    "pingpong.PingPongAkkaActorBenchmark": "Ping Pong",
    "piprecision.PiPrecisionAkkaActorBenchmark": "Precise Pi Calculation",
    "quicksort.QuickSortAkkaGCActorBenchmark": "Recursive Tree Quicksort",
    "radixsort.RadixSortAkkaGCActorBenchmark": "Radix Sort",
    "recmatmul.MatMulAkkaGCActorBenchmark": "Recursive Matrix Multiplication",
    "sieve.SieveAkkaActorBenchmark": "Sieve of Eratosthenes",
    #"sor.SucOverRelaxAkkaActorBenchmark": "Successive Over-Relaxation",       # Skipped due to deadlocks
    "threadring.ThreadRingAkkaActorBenchmark": "Thread Ring",
    "trapezoid.TrapezoidalAkkaActorBenchmark": "Trapezoidal Approximation",
    "uct.UctAkkaActorBenchmark": "Unbalanced Cobwebbed Tree",
}
microBenchmarks = [
    "big.BigAkkaActorBenchmark",
    "chameneos.ChameneosAkkaActorBenchmark",
    "count.CountingAkkaGCActorBenchmark",
    "fib.FibonacciAkkaGCActorBenchmark",
    "fjcreate.ForkJoinAkkaActorBenchmark",
    "fjthrput.ThroughputAkkaActorBenchmark",
    "pingpong.PingPongAkkaActorBenchmark",
    "threadring.ThreadRingAkkaActorBenchmark",
]
concurrentBenchmarks = [
    "banking.BankingAkkaManualStashActorBenchmark",
    "bndbuffer.ProdConsAkkaActorBenchmark",
    "cigsmok.CigaretteSmokerAkkaActorBenchmark",
    "concdict.DictionaryAkkaActorBenchmark",
    "concsll.SortedListAkkaActorBenchmark",
    "logmap.LogisticMapAkkaManualStashActorBenchmark",
    "philosopher.PhilosopherAkkaActorBenchmark",
]
parallelBenchmarks = [
    "apsp.ApspAkkaGCActorBenchmark",
    "astar.GuidedSearchAkkaGCActorBenchmark",
    "bitonicsort.BitonicSortAkkaActorBenchmark",
    "facloc.FacilityLocationAkkaActorBenchmark",
    "nqueenk.NQueensAkkaGCActorBenchmark",
    "piprecision.PiPrecisionAkkaActorBenchmark",
    "quicksort.QuickSortAkkaGCActorBenchmark",
    "radixsort.RadixSortAkkaGCActorBenchmark",
    "recmatmul.MatMulAkkaGCActorBenchmark",
    "sieve.SieveAkkaActorBenchmark",
    "trapezoid.TrapezoidalAkkaActorBenchmark",
    "uct.UctAkkaActorBenchmark",
]


############################## BENCHMARK RUNNER ##############################

def raw_time_filename(benchmark, gc_type):
    return f"raw_data/{benchmark}-{gc_type}.csv"

def run_benchmark(benchmark, gc_type, options, args):
    classname = "edu.rice.habanero.benchmarks." + benchmark

    gc_args = []
    if gc_type == "nogc":
        gc_args = ["-Duigc.engine=manual"]
    elif gc_type == "wrc":
        gc_args = ["-Duigc.engine=mac", "-Duigc.mac.cycle-detection=off"]
    elif gc_type == "mac":
        gc_args = ["-Duigc.engine=mac", "-Duigc.mac.cycle-detection=on"]
    elif gc_type == "crgc-onblock":
        gc_args = ["-Duigc.crgc.collection-style=on-block", "-Duigc.engine=crgc"]
    elif gc_type == "crgc-wave":
        gc_args = ["-Duigc.crgc.collection-style=wave", "-Duigc.engine=crgc"]
    else:
        print(f"Invalid garbage collector type '{gc_type}'. Valid options are: {gc_types}")
        sys.exit(1)

    with open(f'logs/{benchmark}-{gc_type}.log', 'a') as log:
        print(f"Running {benchmark} with {gc_type} garbage collector...")
        start_time = time()
        subprocess.run(["sbt", "-J-Xmx16G", "-J-XX:+UseZGC"] + gc_args + [f'runMain {classname} -iter {args.iter} {options}'], stdout=log, stderr=log)
        end_time = time()
        print(f"Finished {args.iter} iterations in {end_time - start_time:.2f} seconds.")

def run_time_benchmark(benchmark, gc_type, args):
    filename = raw_time_filename(benchmark, gc_type)
    run_benchmark(benchmark, gc_type, f"-filename {filename}", args)


############################## DATA PROCESSING ##############################

def processed_time_filename(benchmark):
    return f"processed_data/{benchmark}.csv"

def get_time_stats(benchmark, gc_type):
    """
    Read the CSV file and return the average and standard deviation.
    """
    filename = raw_time_filename(benchmark, gc_type)
    with open(filename) as file:
        lines = [float(line) for line in file]
        # Only keep the 40% lowest values
        lines = sorted(lines)[:int(len(lines) * 0.4)]
        return np.average(lines), np.std(lines)

def process_time_data(benchmark):
    d = []

    nogc_avg, nogc_std = get_time_stats(benchmark, "nogc")
    d.append(nogc_avg)
    d.append(nogc_std)

    wrc_avg, wrc_std = get_time_stats(benchmark, "wrc")
    d.append(wrc_avg)
    d.append(wrc_std)

    onblk_avg, onblk_std = get_time_stats(benchmark, "crgc-onblock")
    d.append(onblk_avg)
    d.append(onblk_std)

    wave_avg, wave_std = get_time_stats(benchmark, "crgc-wave")
    d.append(wave_avg)
    d.append(wave_std)

    filename = processed_time_filename(benchmark)
    with open(filename, "w") as output:
        output.write('"no GC", "no GC error", "WRC", "WRC error", "CRGC (on-block)", "CRGC error (on-block)", "CRGC (wave)", "CRGC error (wave)"\n')
        output.write(",".join([str(p) for p in d]) + "\n")

def shorten_benchmark_name(benchmark):
    return benchmark.split(".")[0]

def sigfigs(x, n):
    """
    Round x to n significant figures.
    """
    y = round(x, n - int(np.floor(np.log10(abs(x)))) - 1)
    return int(y) if y.is_integer() else y

def cellcolor(stdev, overhead):
    if overhead < 0:
        return f"\\cellcolor{{green!{abs(overhead) / 200 * 60}}}{overhead}"
    else:
        return f"\\cellcolor{{red!{overhead / 200 * 60}}}{overhead}"

def process_all_times(benchmark_list):
    d = {}
    for bm in benchmark_list:
        d[bm] = ["\\texttt{" + shorten_benchmark_name(bm) + "}"]

        nogc_avg, nogc_std = get_time_stats(bm, "nogc")
        d[bm].append(sigfigs(nogc_avg / 1000, 2))

        wrc_avg, _ = get_time_stats(bm, "wrc")
        d[bm].append(sigfigs(wrc_avg / 1000, 2))

        onblk_avg, onblk_std = get_time_stats(bm, "crgc-onblock")
        d[bm].append(sigfigs(onblk_avg / 1000, 2))

        wave_avg, wave_std = get_time_stats(bm, "crgc-wave")
        d[bm].append(sigfigs(wave_avg / 1000, 2))

        percent_stdev = int(nogc_std / nogc_avg * 100)
        d[bm].append("±" + str(percent_stdev))
        d[bm].append(cellcolor(percent_stdev, int((wrc_avg / nogc_avg - 1) * 100)))
        d[bm].append(cellcolor(percent_stdev, int((onblk_avg / nogc_avg - 1) * 100)))
        d[bm].append(cellcolor(percent_stdev, int((wave_avg / nogc_avg - 1) * 100)))

    with open("processed_data/savina.tex", "w") as output:
        output.write('Benchmark, no GC, WRC, CRGC-block, CRGC-wave, no GC (stdev), WRC, CRGC-block, CRGC-wave"\n')
        for bm in microBenchmarks:
            output.write(" & ".join([str(p) for p in d[bm]]) + "\\\\\n")
        for bm in concurrentBenchmarks:
            output.write(" & ".join([str(p) for p in d[bm]]) + "\\\\\n")
        for bm in parallelBenchmarks:
            output.write(" & ".join([str(p) for p in d[bm]]) + "\\\\\n")

def display_data(benchmark_list):
    frames = []

    # Add benchmark data to the dataframe
    for bm in benchmark_list:
        nogc_avg, nogc_std   = get_time_stats(bm, "nogc")
        wrc_avg, _           = get_time_stats(bm, "wrc")
        onblk_avg, onblk_std = get_time_stats(bm, "crgc-onblock")
        wave_avg, wave_std   = get_time_stats(bm, "crgc-wave")

        percent_stdev = int(nogc_std / nogc_avg * 100)
        df = pd.DataFrame({
            "Benchmark":      [shorten_benchmark_name(bm)],
            "no GC":          [sigfigs(nogc_avg / 1000, 2)],
            "WRC":            [sigfigs(wrc_avg / 1000, 2)],
            "CRGC-block":     [sigfigs(onblk_avg / 1000, 2)],
            "CRGC-wave":      [sigfigs(wave_avg / 1000, 2)],
            "no GC (stdev)":  ["±" + str(percent_stdev)],
            "WRC (%)":        [int((wrc_avg / nogc_avg - 1) * 100)],
            "CRGC-block (%)": [int((onblk_avg / nogc_avg - 1) * 100)],
            "CRGC-wave (%)":  [int((wave_avg / nogc_avg - 1) * 100)],
        })
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    print(df.to_markdown(tablefmt="rounded_grid", index=False, headers=df.columns))

############################## RUNNER ##############################

class BenchmarkRunner:

    def __init__(self, benchmarks, gc_types, args):
        self.benchmarks = benchmarks
        self.gc_types = gc_types
        self.args = args

    def run_benchmarks(self):
        for i in range(self.args.invocations):
            for benchmark in self.benchmarks:
                for gc_type in self.gc_types:
                    run_time_benchmark(benchmark, gc_type, self.args)

    def process_time_data(self):
        for bm in self.benchmarks:
            process_time_data(bm)
        process_all_times(self.benchmarks)
        display_data(self.benchmarks)


############################## MAIN ##############################

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=["quick", "full", "process"],
        help="Which command to run."
    )
    parser.add_argument(
        "--iter",
        type=int,
        default=20,
        help="Number of times to run each benchmark PER JVM INVOCATION."
    )
    parser.add_argument(
        "--invocations",
        type=int,
        default=6,
        help="Number of JVM invocations to run for each benchmark."
    )
    args = parser.parse_args()


    if args.command in ["quick", "full"]:
        # Create directories if they don't already exist.
        try:
            os.makedirs('logs')
            os.makedirs('raw_data')
            os.makedirs('processed_data')
        except FileExistsError:
            print("Directories `logs`, `raw_data`, or `processed_data` already exist. Aborting.")
            sys.exit(1)

        # Set the benchmarks
        bms = []
        if args.command == "quick":
            bms = [bm for bm in quick_benchmarks]
        elif args.command == "full":
            bms = benchmarks.keys()

        # Run the benchmarks
        runner = BenchmarkRunner(bms, gc_types, args)
        runner.run_benchmarks()
        runner.process_time_data()

    elif args.command == "process":
        bms = benchmarks.keys()
        runner = BenchmarkRunner(bms, gc_types, args)
        runner.process_time_data()

    else:
        parser.print_help()

