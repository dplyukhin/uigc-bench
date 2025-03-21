#!/usr/bin/env python3

import argparse
import subprocess
import sys
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time
import json

################################################################################
#                                CONFIGURATION                                 #
################################################################################

# PyPlot style
plt.style.use('tableau-colorblind10')

# Which types of garbage collectors to use
gc_types = ["nogc", "wrc", "crgc-onblock", "crgc-wave"]

##################################################
#                     SAVINA                     #
##################################################

savina_jvm_args = ["-J-Xmx8G", "-J-XX:+UseZGC"]

# List of Savina benchmarks to run in the "kick the tires" mode.
savina_kick_benchmarks = [
    "fib.FibonacciAkkaGCActorBenchmark",
]

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

workers_jvm_args = ["-J-Xmx2G", "-J-XX:+UseZGC"]

workers_modes = ["torture-small", "torture-large", "streaming"]

torture_small = {
}

torture_large = {
    "random-workers.max-work-size-in-bytes": 5120
}

streaming_mode = {
    "random-workers.max-work-size-in-bytes": 5120,
    "random-workers.wrk-probabilities.spawn": 0,
    "random-workers.wrk-probabilities.acquaint": 0,
    "random-workers.mgr-probabilities.spawn": 0.01,
    "random-workers.mgr-probabilities.local-acquaint": 0,
    "random-workers.mgr-probabilities.publish-worker": 1,
    "random-workers.mgr-probabilities.deactivate": 0,
}

acyclic_mode = {
    "random-workers.acyclic": "true",
}


################################################################################
#                                   RUNNER                                     #
################################################################################

def get_gc_args(gc_type, num_nodes=1):
    if gc_type == "nogc":
        return ["-Duigc.engine=manual"]
    elif gc_type == "wrc":
        return ["-Duigc.engine=wrc"]
    elif gc_type == "crgc-onblock":
        return ["-Duigc.engine=crgc", f"-Duigc.crgc.num-nodes={num_nodes}", "-Duigc.crgc.collection-style=on-block"]
    elif gc_type == "crgc-wave":
        return ["-Duigc.engine=crgc", f"-Duigc.crgc.num-nodes={num_nodes}", "-Duigc.crgc.collection-style=wave"]
    else:
        print(f"Invalid garbage collector type '{gc_type}'. Valid options are: {gc_types}")
        sys.exit(1)

##################################################
#                     SAVINA                     #
##################################################

def raw_time_filename(benchmark, data_dir, gc_type):
    return f"{data_dir}/raw/{benchmark}-{gc_type}.csv"

def savina_run_benchmark(benchmark, gc_type, data_dir, args):
    filename = raw_time_filename(benchmark, data_dir, gc_type)
    classname = "edu.rice.habanero.benchmarks." + benchmark
    gc_args = get_gc_args(gc_type)

    with open(f'{data_dir}/logs/{benchmark}-{gc_type}.log', 'a') as log:
        print(f"Running {short_name(benchmark)} with {gc_type}...", end=" ", flush=True)
        start_time = time.time()
        subprocess.run(["sbt"] + savina_jvm_args + gc_args + [f'savina/runMain {classname} -iter {args.iterations} -filename {filename}'], stdout=log, stderr=log)
        end_time = time.time()
        print(f"Finished in {end_time - start_time:.2f} seconds.")

##################################################
#                     WORKERS                    #
##################################################

class WorkersRunInfo:
    def __init__(self, rps, data_dir, mode, gc_type, num_nodes, iterations):
        self.rps = rps
        self.data_dir = data_dir
        self.mode = mode
        self.gc_type = gc_type
        self.num_nodes = num_nodes
        self.iterations = iterations

    def log(self, role):
        return f'{self.data_dir}/logs/workers-{self.rps}-{self.mode}-{self.gc_type}-{role}.log'

    def jfr(self, role):
        return f'{self.data_dir}/raw/workers-{self.rps}-{self.mode}-{self.gc_type}-{role}.jfr'

    def json(self, role):
        return f'{self.data_dir}/raw/workers-{self.rps}-{self.mode}-{self.gc_type}-{role}.json'

    def lifetimes(self):
        return f'{self.data_dir}/raw/workers-{self.rps}-{self.mode}-{self.gc_type}-lifetimes.csv'

    def queries(self):
        return f'{self.data_dir}/raw/workers-{self.rps}-{self.mode}-{self.gc_type}-queries.csv'

    def gc_args(self):
        return get_gc_args(self.gc_type, self.num_nodes)

    def workers_args(self):
        d = None
        if self.mode == "torture-small":
            d = torture_small
        elif self.mode == "torture-large":
            d = torture_large
        elif self.mode == "streaming":
            d = streaming_mode
        else:
            print(f"Invalid random workers mode '{self.mode}'. Valid options are: {workers_modes}")
            sys.exit(1)

        return [f"-D{key}={value}" for key, value in d.items()] + [
            f"-Drandom-workers.life-times-file={self.lifetimes()}",
            f"-Drandom-workers.query-times-file={self.queries()}",
            f"-Drandom-workers.reqs-per-second={self.rps}",
            f"-Dbench.iterations={self.iterations}",
        ]

def workers_run_local(info):
    with open(info.log("orchestrator"), 'w') as log:
        print(f"Running RandomWorkers in {info.mode} mode with {info.gc_type}...", end=" ", flush=True)
        start_time = time.time()

        process = subprocess.Popen(
            ["sbt"] +
            workers_jvm_args +
            info.gc_args() +
            info.workers_args() +
            [f"workers/run 1 orchestrator 0.0.0.0 0.0.0.0"],
            stdout=log, stderr=log
        )
        # Wait for the orchestrator to terminate
        process.wait()

        end_time = time.time()
        print(f"Finished in {end_time - start_time:.2f} seconds.")

def workers_run_cluster(info):
    # Add JFR options to SBT opts, saving the old value to be restored later.
    original_sbt_opts = os.environ.get("SBT_OPTS", "")

    with open(info.log("orchestrator"), 'w') as log1, \
         open(info.log("manager1"), 'w') as log2, \
         open(info.log("manager2"), 'w') as log3:

        logs = {
            "orchestrator": log1,
            "manager1": log2,
            "manager2": log3,
        }

        print(f"Running RandomWorkers in {info.mode} mode with {info.gc_type}...", end=" ", flush=True)
        start_time = time.time()

        processes = []
        for role in ["orchestrator", "manager1", "manager2"]:
            os.environ["SBT_OPTS"] = original_sbt_opts + \
                 f" -XX:StartFlightRecording=filename={info.jfr(role)},settings=profile.jfc,dumponexit=true"

            process = subprocess.Popen(
                ["sbt"] +
                workers_jvm_args +
                info.gc_args() +
                info.workers_args() +
                [f"workers/run 3 {role} 0.0.0.0 0.0.0.0"],
                stdout=logs[role], stderr=logs[role]
            )
            processes.append(process)
            time.sleep(5)

        # Wait for all processes to terminate
        for process in processes:
            process.wait()

        end_time = time.time()
        print(f"Finished in {end_time - start_time:.2f} seconds.")

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
    if np.isnan(x):
        return x
    y = round(x, n - int(np.floor(np.log10(abs(x)))) - 1)
    return int(y) if y.is_integer() else y

def to_int(x):
    if np.isnan(x):
        return x
    return int(x)

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

            percent_stdev = to_int(nogc_std / nogc_avg * 100)
            df = pd.DataFrame({
                "Benchmark":        [short_name(bm)],
                "no GC":            [sigfigs(nogc_avg / 1000, 2)],
                "WRC":              [sigfigs(wrc_avg / 1000, 2)],
                "CRGC-block":       [sigfigs(onblk_avg / 1000, 2)],
                "CRGC-wave":        [sigfigs(wave_avg / 1000, 2)],
                "no GC (stdev %)":  ["±" + str(percent_stdev)],
                "WRC (%)":          [to_int((wrc_avg / nogc_avg - 1) * 100)],
                "CRGC-block (%)":   [to_int((onblk_avg / nogc_avg - 1) * 100)],
                "CRGC-wave (%)":    [to_int((wave_avg / nogc_avg - 1) * 100)],
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

    return micro_df, concurrency_df, parallel_df

##################################################
#                     WORKERS                    #
##################################################

def bytes_to_str(n):
    if n < 1024:
        return f"{n} B"
    n /= 1024
    if n < 1024:
        return f"{n:.2f} KB"
    n /= 1024
    if n < 1024:
        return f"{n:.2f} MB"
    n /= 1024
    return f"{n:.2f} GB"

def workers_process_data(info):
    app = 0
    refs = 0
    shadows = 0
    ingress = 0

    for role in ["orchestrator", "manager1", "manager2"]:
        jfr_file = info.jfr(role)
        json_file = info.json(role)

        if not os.path.exists(jfr_file):
            raise FileNotFoundError(f"JFR file not found: {jfr_file}")

        subprocess.run(f"jfr print --categories 'UIGC' --json {jfr_file} > {json_file}", shell=True)

        with open(json_file, 'r') as f:
            data = json.load(f)
            events = data['recording']['events']
            for event in events:
                if "IngressEntrySerialization" in event['type']:
                    ingress += event['values']['size']
                elif "DeltaGraphSerialization" in event['type']:
                    refs += event['values']['compressionTableSize']
                    shadows += event['values']['shadowSize']
                elif "AppMsgSerialization" in event['type']:
                    app += event['values']['size']

        os.remove(json_file)

    total = app + refs + shadows + ingress

    app_bytes = bytes_to_str(app)
    refs_bytes = bytes_to_str(refs)
    shadows_bytes = bytes_to_str(shadows)
    ingress_bytes = bytes_to_str(ingress)

    app_ratio = (app / total) * 100
    refs_ratio = (refs / total) * 100
    shadows_ratio = (shadows / total) * 100
    ingress_ratio = (ingress / total) * 100

    df = pd.DataFrame({
        "Mode":             [info.mode],
        "Application data": [f"{app_bytes} ({app_ratio:.2f}%)"],
        "Actor references": [f"{refs_bytes} ({refs_ratio:.2f}%)"],
        "Shadows":          [f"{shadows_bytes} ({shadows_ratio:.2f}%)"],
        "Ingress entries":  [f"{ingress_bytes} ({ingress_ratio:.2f}%)"],
    })
    return df

def workers_display_data(data_dir):
    df = pd.DataFrame()
    for mode in workers_modes:
        for rps in [200]:
            for gc_type in ["crgc-onblock"]:
                info = WorkersRunInfo(
                    rps=rps, data_dir=data_dir, mode=mode, gc_type=gc_type, num_nodes=3, iterations=1
                )
                try:
                    frame = workers_process_data(info)
                    df = pd.concat([df, frame], ignore_index=True)
                except:
                    continue

    return df


############################## MAIN ##############################

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=["kick", "quick", "full", "view"],
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
    parser.add_argument(
        "--rw-modes",
        type=str,
        default=None,
        help="List of Random Workers modes to run, as a comma-separated list. Options are 'torture-small', 'torture-large', or 'streaming'.",
    )
    args = parser.parse_args()

    os.makedirs('data', exist_ok=True)

    if args.command in ["kick", "quick", "full"]:
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
        if args.command == "kick":
            savina_benchmarks = savina_kick_benchmarks
            if args.iterations is None:
                args.iterations = 5
            if args.invocations is None:
                args.invocations = 1
            if args.rw_modes is None:
                args.rw_modes = "streaming"
        elif args.command == "quick":
            savina_benchmarks = savina_quick_benchmarks
            if args.iterations is None:
                args.iterations = 10
            if args.invocations is None:
                args.invocations = 1
            if args.rw_modes is None:
                args.rw_modes = "torture-small,torture-large,streaming"
        elif args.command == "full":
            savina_benchmarks = savina_all_benchmarks
            if args.iterations is None:
                args.iterations = 20
            if args.invocations is None:
                args.invocations = 6
            if args.rw_modes is None:
                args.rw_modes = "torture-small,torture-large,streaming"

        start_time = time.time()
        # Run the Savina benchmarks
        for i in range(args.invocations):
            for benchmark in savina_benchmarks:
                for gc_type in gc_types:
                    savina_run_benchmark(benchmark, gc_type, data_dir, args)
        # Run the Random Workers benchmark
        for mode in args.rw_modes.split(","):
            for rps in [200]:
                for gc_type in ["crgc-onblock"]:
                    info = WorkersRunInfo(
                        rps=rps, data_dir=data_dir, mode=mode, gc_type=gc_type, num_nodes=3, iterations=1
                    )
                    workers_run_cluster(info)
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

        micro_df, concurrency_df, parallel_df = savina_display_data(data_dir)
        rw_df = workers_display_data(data_dir)

        if not micro_df.empty:
            print("\nSavina Microbenchmarks:")
            print(micro_df.to_markdown(tablefmt="rounded_grid", index=False))
        if not concurrency_df.empty:
            print("\nSavina Concurrency Benchmarks:")
            print(concurrency_df.to_markdown(tablefmt="rounded_grid", index=False))
        if not parallel_df.empty:
            print("\nSavina Parallel Benchmarks:")
            print(parallel_df.to_markdown(tablefmt="rounded_grid", index=False))
        if not rw_df.empty:
            print("\nRandom Workers Bandwidth:")
            print(rw_df.to_markdown(tablefmt="rounded_grid", index=False))

    else:
        parser.print_help()

