#!/usr/bin/env python3

import argparse
import subprocess
import sys
import os
import numpy as np
import pandas as pd
import time

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
benchmark_names = {
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

def raw_time_filename(benchmark, data_dir, gc_type):
    return f"{data_dir}/raw/{benchmark}-{gc_type}.csv"

def run_benchmark(benchmark, gc_type, data_dir, args):
    filename = raw_time_filename(benchmark, data_dir, gc_type)
    classname = "edu.rice.habanero.benchmarks." + benchmark

    gc_args = []
    if gc_type == "nogc":
        gc_args = ["-Duigc.engine=manual"]
    elif gc_type == "wrc":
        gc_args = ["-Duigc.engine=mac", "-Duigc.mac.cycle-detection=off"]
    elif gc_type == "crgc-onblock":
        gc_args = ["-Duigc.crgc.collection-style=on-block", "-Duigc.engine=crgc"]
    elif gc_type == "crgc-wave":
        gc_args = ["-Duigc.crgc.collection-style=wave", "-Duigc.engine=crgc"]
    else:
        print(f"Invalid garbage collector type '{gc_type}'. Valid options are: {gc_types}")
        sys.exit(1)

    with open(f'{data_dir}/logs/{benchmark}-{gc_type}.log', 'a') as log:
        print(f"Running {shorten_benchmark_name(benchmark)} with {gc_type}...", end=" ", flush=True)
        start_time = time.time()
        subprocess.run(["sbt", "-J-Xmx8G", "-J-XX:+UseZGC"] + gc_args + [f'savina/runMain {classname} -iter {args.iter} -filename {filename}'], stdout=log, stderr=log)
        end_time = time.time()
        print(f"Finished in {end_time - start_time:.2f} seconds.")


############################## DATA PROCESSING ##############################

def get_time_stats(benchmark, data_dir, gc_type):
    """
    Read the CSV file and return the average and standard deviation.
    """
    filename = raw_time_filename(benchmark, data_dir, gc_type)
    with open(filename) as file:
        lines = [float(line) for line in file]
        # Only keep the 40% lowest values
        lines = sorted(lines)[:int(len(lines) * 0.4)]
        if len(lines) == 0:
            raise ValueError(f"Insufficient data in {filename}")
        return np.average(lines), np.std(lines)

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

def process_all_times(benchmark_list, data_dir):
    d = {}
    for bm in benchmark_list:
        d[bm] = ["\\texttt{" + shorten_benchmark_name(bm) + "}"]

        nogc_avg, nogc_std = get_time_stats(bm, data_dir, "nogc")
        d[bm].append(sigfigs(nogc_avg / 1000, 2))

        wrc_avg, _ = get_time_stats(bm, data_dir, "wrc")
        d[bm].append(sigfigs(wrc_avg / 1000, 2))

        onblk_avg, onblk_std = get_time_stats(bm, data_dir, "crgc-onblock")
        d[bm].append(sigfigs(onblk_avg / 1000, 2))

        wave_avg, wave_std = get_time_stats(bm, data_dir, "crgc-wave")
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

def display_data(data_dir):
    micro_df       = pd.DataFrame()
    concurrency_df = pd.DataFrame()
    parallel_df    = pd.DataFrame()
    missing_data   = []

    # Add benchmark data to the dataframe
    for bm in benchmark_names.keys():
        try:
            nogc_avg, nogc_std   = get_time_stats(bm, data_dir, "nogc")
            wrc_avg, _           = get_time_stats(bm, data_dir, "wrc")
            onblk_avg, onblk_std = get_time_stats(bm, data_dir, "crgc-onblock")
            wave_avg, wave_std   = get_time_stats(bm, data_dir, "crgc-wave")

            percent_stdev = int(nogc_std / nogc_avg * 100)
            df = pd.DataFrame({
                "Benchmark":        [shorten_benchmark_name(bm)],
                "no GC":            [sigfigs(nogc_avg / 1000, 2)],
                "WRC":              [sigfigs(wrc_avg / 1000, 2)],
                "CRGC-block":       [sigfigs(onblk_avg / 1000, 2)],
                "CRGC-wave":        [sigfigs(wave_avg / 1000, 2)],
                "no GC (stdev %)":  ["±" + str(percent_stdev)],
                "WRC (%)":          [int((wrc_avg / nogc_avg - 1) * 100)],
                "CRGC-block (%)":   [int((onblk_avg / nogc_avg - 1) * 100)],
                "CRGC-wave (%)":    [int((wave_avg / nogc_avg - 1) * 100)],
            })
            if bm in microBenchmarks:
                micro_df = pd.concat([micro_df, df], ignore_index=True)
            elif bm in concurrentBenchmarks:
                concurrency_df = pd.concat([concurrency_df, df], ignore_index=True)
            elif bm in parallelBenchmarks:
                parallel_df = pd.concat([parallel_df, df], ignore_index=True)
        except:
            missing_data.append(bm)
            continue

    if len(missing_data) > 0:
        missing_data = ", ".join([shorten_benchmark_name(bm) for bm in missing_data])
        print(f"Missing data for the following benchmarks: {missing_data}")

    print("Microbenchmarks:")
    print(micro_df.to_markdown(tablefmt="rounded_grid", index=False))
    print("\nConcurrency benchmarks:")
    print(concurrency_df.to_markdown(tablefmt="rounded_grid", index=False))
    print("\nParallel benchmarks:")
    print(parallel_df.to_markdown(tablefmt="rounded_grid", index=False))


############################## MAIN ##############################

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=["quick", "full", "view"],
        help="Which command to run."
    )
    parser.add_argument(
        "--iter",
        type=int,
        default=None,
        help="Number of times to run each benchmark PER JVM INVOCATION."
    )
    parser.add_argument(
        "--invocations",
        type=int,
        default=None,
        help="Number of JVM invocations to run for each benchmark."
    )
    args = parser.parse_args()

    os.makedirs('data', exist_ok=True)

    if args.command in ["quick", "full"]:
        # Current time in the form YYYY-MM-DD-HH-MM-SS
        timestamp = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        data_dir = f"data/{timestamp}"

        # Create directories if they don't already exist.
        try:
            os.makedirs(f'{data_dir}')
            os.makedirs(f'{data_dir}/logs')
            os.makedirs(f'{data_dir}/raw')
        except FileExistsError:
            print(f"Directory `{data_dir}` already exists. Aborting.")
            sys.exit(1)

        # Set the benchmarks
        benchmarks = []
        if args.command == "quick":
            benchmarks = quick_benchmarks
            if args.iter is None:
                args.iter = 10
            if args.invocations is None:
                args.invocations = 1
        elif args.command == "full":
            benchmarks = benchmark_names.keys()
            if args.iter is None:
                args.iter = 20
            if args.invocations is None:
                args.invocations = 6

        # Run the benchmarks
        start_time = time.time()
        for i in range(args.invocations):
            for benchmark in benchmarks:
                for gc_type in gc_types:
                    run_benchmark(benchmark, gc_type, data_dir, args)
        end_time = time.time()
        print(f"Finished everything in {(end_time - start_time)/60:.2f} minutes.")

    elif args.command == "view":
        # Get a list of directories in the data folder
        directories = [d for d in os.listdir("data") if os.path.isdir(f"data/{d}")]
        # Sort the directories by modification time
        directories.sort(key=lambda x: os.path.getmtime(f"data/{x}"))

        if len(directories) == 0:
            print("No data found in the `data/` directory.")
            sys.exit(1)

        # Choose a directory to look at
        data_dir = None
        if len(directories) == 1:
            data_dir = f"data/{directories[0]}"
        else:
            # Ask the user to pick a directory
            print("Multiple runs found in the `data/` directory. Please choose one:")
            for i, d in enumerate(directories):
                print(f"[{i}] {d}")
            while True:
                choice = input("Choose a run, or press Enter for the most recent run: ")
                if choice == "":
                    data_dir = f"data/{directories[-1]}"
                    break
                try:
                    choice = int(choice)
                    if choice not in range(len(directories)):
                        raise ValueError
                    data_dir = f"data/{directories[choice]}"
                    break
                except ValueError:
                    print("Invalid choice. Please enter a number or press Enter.")

        display_data(data_dir)

    else:
        parser.print_help()

