#!/usr/bin/env python3

import argparse
import subprocess
import sys
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time

################################################################################
#                                CONFIGURATION                                 #
################################################################################

##################################################
#                     SAVINA                     #
##################################################

# Which types of garbage collectors to use
gc_types = ["nogc", "wrc", "crgc-onblock", "crgc-wave"]

# List of Savina benchmarks to run in the "quick" evaluation.
# Benchmarks that take more than a second to run are excluded.
savina_quick_benchmarks = [
    #### Microbenchmarks
    "fib.FibonacciAkkaGCActorBenchmark",
    "fjthrput.ThroughputAkkaActorBenchmark",
    "threadring.ThreadRingAkkaActorBenchmark",

    ### Concurrent benchmarks
    "banking.BankingAkkaManualStashActorBenchmark",
    "bndbuffer.ProdConsAkkaActorBenchmark",
    "cigsmok.CigaretteSmokerAkkaActorBenchmark",
    "concdict.DictionaryAkkaActorBenchmark",
    "logmap.LogisticMapAkkaManualStashActorBenchmark",

    ### Parallel benchmarks
    "apsp.ApspAkkaGCActorBenchmark",
    "bitonicsort.BitonicSortAkkaActorBenchmark",
    "facloc.FacilityLocationAkkaActorBenchmark",
    "nqueenk.NQueensAkkaGCActorBenchmark",
    "piprecision.PiPrecisionAkkaActorBenchmark",
    "quicksort.QuickSortAkkaGCActorBenchmark",
    "recmatmul.MatMulAkkaGCActorBenchmark",
    "sieve.SieveAkkaActorBenchmark",
    "trapezoid.TrapezoidalAkkaActorBenchmark",
    "uct.UctAkkaActorBenchmark",
]

savina_microbenchmarks = [
    "big.BigAkkaActorBenchmark",
    "chameneos.ChameneosAkkaActorBenchmark",
    "count.CountingAkkaGCActorBenchmark",
    "fib.FibonacciAkkaGCActorBenchmark",
    "fjcreate.ForkJoinAkkaActorBenchmark",
    "fjthrput.ThroughputAkkaActorBenchmark",
    "pingpong.PingPongAkkaActorBenchmark",
    "threadring.ThreadRingAkkaActorBenchmark",
]

savina_concurrent_benchmarks = [
    "banking.BankingAkkaManualStashActorBenchmark",
    #"barber.SleepingBarberAkkaActorBenchmark",            # Skipped due to unstable performance
    "bndbuffer.ProdConsAkkaActorBenchmark",
    "cigsmok.CigaretteSmokerAkkaActorBenchmark",
    "concdict.DictionaryAkkaActorBenchmark",
    "concsll.SortedListAkkaActorBenchmark",
    "logmap.LogisticMapAkkaManualStashActorBenchmark",
    "philosopher.PhilosopherAkkaActorBenchmark",
]

savina_parallel_benchmarks = [
    "apsp.ApspAkkaGCActorBenchmark",
    "astar.GuidedSearchAkkaGCActorBenchmark",
    "bitonicsort.BitonicSortAkkaActorBenchmark",
    "facloc.FacilityLocationAkkaActorBenchmark",
    #"filterbank.FilterBankAkkaActorBenchmark",            # Skipped due to deadlocks
    "nqueenk.NQueensAkkaGCActorBenchmark",
    "piprecision.PiPrecisionAkkaActorBenchmark",
    "quicksort.QuickSortAkkaGCActorBenchmark",
    "radixsort.RadixSortAkkaGCActorBenchmark",
    "recmatmul.MatMulAkkaGCActorBenchmark",
    "sieve.SieveAkkaActorBenchmark",
    #"sor.SucOverRelaxAkkaActorBenchmark",                 # Skipped due to deadlocks
    "trapezoid.TrapezoidalAkkaActorBenchmark",
    "uct.UctAkkaActorBenchmark",
]

# List of all Savina benchmarks
savina_all_benchmarks = savina_microbenchmarks + savina_concurrent_benchmarks + savina_parallel_benchmarks


##################################################
#                     WORKERS                    #
##################################################

default_mode = {
}

big_data_mode = {
    "random-workers.max-work-size-in-bytes": 5120
}

streaming_mode = {
    **big_data_mode,
    "random-workers.wrk-probabilities.spawn": 0,
    "random-workers.wrk-probabilities.acquaint": 0,
    "random-workers.mgr-probabilities.spawn": 0.01,
    "random-workers.mgr-probabilities.local-acquaint": 0,
    "random-workers.mgr-probabilities.publish-worker": 1,
    "random-workers.mgr-probabilities.deactivate": 0,
    "random-workers.total-queries": 20000,
}

acyclic_mode = {
    "random-workers.total-queries": 500,
    "random-workers.max-acqs-per-msg": 0,
    "random-workers.mgr-probabilities.local-acquaint": 0,
    "random-workers.wrk-probabilities.acquaint": 0,
}

local_acyclic_mode = {
    **acyclic_mode,
    "random-workers.acyclic": "true",
}

local_cyclic_mode = {
    **acyclic_mode,
    "random-workers.acyclic": "false",
}


################################################################################
#                                   RUNNER                                     #
################################################################################

##################################################
#                     SAVINA                     #
##################################################

def raw_time_filename(benchmark, data_dir, gc_type):
    return f"{data_dir}/raw/{benchmark}-{gc_type}.csv"

def savina_run_benchmark(benchmark, gc_type, data_dir, args):
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
        print(f"Running {short_name(benchmark)} with {gc_type}...", end=" ", flush=True)
        start_time = time.time()
        subprocess.run(["sbt", "-J-Xmx8G", "-J-XX:+UseZGC"] + gc_args + [f'savina/runMain {classname} -iter {args.iterations} -filename {filename}'], stdout=log, stderr=log)
        end_time = time.time()
        print(f"Finished in {end_time - start_time:.2f} seconds.")

##################################################
#                     WORKERS                    #
##################################################

def mode_to_args(mode):
    """Converts a mode into a list of arguments to be passed to the JVM."""
    return [f"-D{key}={value}" for key, value in mode.items()]

def workers_run_local(reqs_per_second, output_dir, mode):
    for gc_type in ["crgc", "mac", "manual"]:
        filename = f"workers-rps{reqs_per_second}-{gc_type}"
        with open(f'{output_dir}/{filename}.log', 'w') as log:
            process = subprocess.Popen(
                ["sbt", "-J-Xmx2G", "-J-XX:+UseZGC", f"-Duigc.engine={gc_type}", "-Duigc.crgc.num-nodes=1",
                 f"-Drandom-workers.life-times-file=life-times-{gc_type}.csv",
                 f"-Drandom-workers.reqs-per-second={reqs_per_second}"] +
                mode_to_args(mode) +
                [f"runMain randomworkers.RandomWorkers 1 orchestrator 0.0.0.0 0.0.0.0"],
                stdout=log
            )
            # Wait for the orchestrator to terminate
            process.wait()

def workers_run_cluster(reqs_per_second, data_dir, mode):
    filename = f"workers-rps-{reqs_per_second}"

    # Add JFR options to SBT opts, saving the old value to be restored later.
    original_sbt_opts = os.environ.get("SBT_OPTS", "")

    with open(f'{data_dir}/logs/{filename}-orchestrator.log', 'w') as log1, \
         open(f'{data_dir}/logs/{filename}-manager1.log', 'w') as log2, \
         open(f'{data_dir}/logs/{filename}-manager2.log', 'w') as log3:

        logs = {
            "orchestrator": log1,
            "manager1": log2,
            "manager2": log3,
        }

        processes = []
        for role in ["orchestrator", "manager1", "manager2"]:
            jfr_file = f"{data_dir}/raw/{filename}-{role}.jfr"
            jfr_settings = 'profile.jfc'

            os.environ["SBT_OPTS"] = original_sbt_opts + \
                 f" -XX:StartFlightRecording=filename={jfr_file},settings={jfr_settings},dumponexit=true"

            process = subprocess.Popen(
                ["sbt", "-J-Xmx2G", "-J-XX:+UseZGC", "-Duigc.engine=crgc", "-Duigc.crgc.num-nodes=3",
                 f"-Drandom-workers.reqs-per-second={reqs_per_second}"] +
                mode_to_args(mode) +
                [f"workers/run 3 {role} 0.0.0.0 0.0.0.0"],
                stdout=logs[role],
                stderr=logs[role]
            )
            processes.append(process)
            time.sleep(5)

        # Wait for all processes to terminate
        for process in processes:
            process.wait()

    # Restore the original SBT_OPTS for the next iteration
    os.environ["SBT_OPTS"] = original_sbt_opts


################################################################################
#                                 PROCESSING                                   #
################################################################################

def get_time_stats(benchmark, data_dir, gc_type):
    """Read the CSV file and return the average and standard deviation."""
    filename = raw_time_filename(benchmark, data_dir, gc_type)
    with open(filename) as file:
        lines = [float(line) for line in file]
        # Only keep the 40% lowest values
        lines = sorted(lines)[:int(len(lines) * 0.4)]
        if len(lines) == 0:
            raise ValueError(f"Insufficient data in {filename}")
        return np.average(lines), np.std(lines)

##################################################
#                     SAVINA                     #
##################################################

def short_name(benchmark):
    return benchmark.split(".")[0]

def sigfigs(x, n):
    """Round x to n significant figures."""
    y = round(x, n - int(np.floor(np.log10(abs(x)))) - 1)
    return int(y) if y.is_integer() else y

def savina_display_data(data_dir):
    micro_df       = pd.DataFrame()
    concurrency_df = pd.DataFrame()
    parallel_df    = pd.DataFrame()
    missing_data   = []

    # Add benchmark data to the dataframe
    for bm in savina_all_benchmarks:
        try:
            nogc_avg, nogc_std   = get_time_stats(bm, data_dir, "nogc")
            wrc_avg, _           = get_time_stats(bm, data_dir, "wrc")
            onblk_avg, onblk_std = get_time_stats(bm, data_dir, "crgc-onblock")
            wave_avg, wave_std   = get_time_stats(bm, data_dir, "crgc-wave")

            percent_stdev = int(nogc_std / nogc_avg * 100)
            df = pd.DataFrame({
                "Benchmark":        [short_name(bm)],
                "no GC":            [sigfigs(nogc_avg / 1000, 2)],
                "WRC":              [sigfigs(wrc_avg / 1000, 2)],
                "CRGC-block":       [sigfigs(onblk_avg / 1000, 2)],
                "CRGC-wave":        [sigfigs(wave_avg / 1000, 2)],
                "no GC (stdev %)":  ["±" + str(percent_stdev)],
                "WRC (%)":          [int((wrc_avg / nogc_avg - 1) * 100)],
                "CRGC-block (%)":   [int((onblk_avg / nogc_avg - 1) * 100)],
                "CRGC-wave (%)":    [int((wave_avg / nogc_avg - 1) * 100)],
            })
            if bm in savina_microbenchmarks:
                micro_df = pd.concat([micro_df, df], ignore_index=True)
            elif bm in savina_concurrent_benchmarks:
                concurrency_df = pd.concat([concurrency_df, df], ignore_index=True)
            elif bm in savina_parallel_benchmarks:
                parallel_df = pd.concat([parallel_df, df], ignore_index=True)
        except:
            missing_data.append(bm)
            continue

    print("Microbenchmarks:")
    print(micro_df.to_markdown(tablefmt="rounded_grid", index=False))
    print("\nConcurrency benchmarks:")
    print(concurrency_df.to_markdown(tablefmt="rounded_grid", index=False))
    print("\nParallel benchmarks:")
    print(parallel_df.to_markdown(tablefmt="rounded_grid", index=False))

##################################################
#                     WORKERS                    #
##################################################

def workers_get_data(filename):
    with open(filename, 'r') as f:
        numbers = [float(line.strip()) for line in f if line.strip()]
    return numbers

def workers_compute_cdf(data):
    sorted_data = np.sort(data)
    cdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    return sorted_data, cdf

def workers_plot_cdf(data1, data2, data3, label1='CRGC', label2='WRC', label3='No GC'):
    x1, cdf1 = workers_compute_cdf(data1)
    x2, cdf2 = workers_compute_cdf(data2)
    x3, cdf3 = workers_compute_cdf(data3)

    plt.figure(figsize=(4, 2))
    step = 10000
    plt.plot(x1[::step], cdf1[::step], label=label1, marker='o', linestyle='-', alpha=0.7)
    plt.plot(x2[::step], cdf2[::step], label=label2, marker='s', linestyle='-', alpha=0.7)
    plt.plot(x3[::step], cdf3[::step], label=label3, marker='d', linestyle='-', alpha=0.7)

    plt.xlim([0, 1500])

    plt.xlabel('Actor life time (ms)')
    plt.legend()
    plt.grid(True)
    plt.show()

def workers_plot():
    file1_data = workers_get_data("life-times-cyclic-crgc.csv")
    file2_data = workers_get_data("life-times-cyclic-mac.csv")
    file3_data = workers_get_data("life-times-cyclic-manual.csv")
    workers_plot_cdf(file1_data, file2_data, file3_data)
    file1_data = workers_get_data("life-times-acyclic-crgc.csv")
    file2_data = workers_get_data("life-times-acyclic-mac.csv")
    file3_data = workers_get_data("life-times-acyclic-manual.csv")
    workers_plot_cdf(file1_data, file2_data, file3_data)


############################## MAIN ##############################

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=["quick", "full", "view"],
        help="Which command to run."
    )
    parser.add_argument(
        "--iterations",
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
        savina_benchmarks = []
        if args.command == "quick":
            savina_benchmarks = savina_quick_benchmarks
            if args.iterations is None:
                args.iterations = 10
            if args.invocations is None:
                args.invocations = 1
        elif args.command == "full":
            savina_benchmarks = savina_all_benchmarks
            if args.iterations is None:
                args.iterations = 20
            if args.invocations is None:
                args.invocations = 6

        # Run the benchmarks
        start_time = time.time()
        #for i in range(args.invocations):
        #    for benchmark in savina_benchmarks:
        #        for gc_type in gc_types:
        #            savina_run_benchmark(benchmark, gc_type, data_dir, args)
        workers_run_cluster(200, data_dir, default_mode)
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

        savina_display_data(data_dir)

    else:
        parser.print_help()

