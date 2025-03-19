# UIGC-Bench

[UIGC](https://github.com/dplyukhin/uigc-pekko) is an actor garbage collector
for Apache Pekko. This repository is a benchmark suite for UIGC. It has two
sub-projects:

1. A fork of the [Savina](https://github.com/shamsimam/savina) actor benchmark
   suite, ported to Pekko/UIGC.
2. A randomized, configurable benchmark called **RandomWorkers**. 

RandomWorkers is based on the [ChatApp](https://github.com/sblessing/chat-app) 
benchmark. But unlike ChatApp, RandomWorkers is designed to run on a cluster and
it generates actor garbage.

## Quick Start

You must have `python3` with `numpy`, `pandas`, and `tabulate` installed.
You also need Java 17+ and at least 8 GB of memory available.

### Quick Evaluation

The "quick" evaluation takes about 30 minutes. Here's how to run it:

```bash
./main.py quick
```

The "quick" evaluation only runs a subset of the Savina benchmarks. Each 
benchmark only runs for ten iterations and just one JVM invocation. You 
can customize the number of iterations and JVM invocations with the 
`--iterations` and `--invocations` flags, respectively. The raw data will be 
written to the `data/` directory.

### Viewing Results

To display the results as an ASCII table, run the following command.

```bash
./main.py view
```

If you ran the evaluation multiple times, the command lets you choose which 
"run" to look at. 

### Full Evaluation

If you have time, run the "full" evaluation. It runs each Savina benchmark for 
20 iterations across 6 JVM invocations and takes about 10 hours. 
Run it with the following command:

```bash
./main.py full
```

You can customize the number of iterations and JVM invocations with the
`--iterations` and `--invocations` flags, respectively.

## Project Overview

### Savina

Benchmarks are located in `savina/src/main/scala/edu/rice/habanero/benchmarks`. 
Each benchmark from the original Savina repository has been ported to use the 
Pekko/UIGC API. Implementations of benchmarks from other actor frameworks have
been removed.

Configuration files for each benchmark are located in 
`savina/src/main/java/edu/rice/habanero/benchmarks`.

### RandomWorkers

The RandomWorkers benchmark is in the `workers/` directory. The default 
configuration parameters are listed in 
`workers/src/main/resources/random-workers.conf`. The benchmark runner 
`main.py` is already configured to run the benchmark in three different
"modes": `torture-small`, `torture-large`, and `streaming`.