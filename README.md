[Savina](https://github.com/shamsimam/savina) is an Actor Benchmark Suite, 
originally developed by Shams Imam and Vivek Sarkar at Rice University.

This repository is a fork of Savina. Benchmarks related to Akka have been 
ported to Pekko. Benchmarks related to other actor frameworks have been removed.

# Quick Start

You must have `python3` with `numpy`, `pandas`, and `tabulate` installed.
You also need Java 17+ and at least 8 GB of memory available.

## Quick Evaluation

The "quick" evaluation takes about 30 minutes. Here's how to run it:

```bash
./savina.py quick
```

The "quick" evaluation only runs a subset of the Savina benchmarks. Each 
benchmark only runs for ten iterations and just one JVM invocation. You 
can customize the number of iterations and JVM invocations with the 
`--iter` and `--invocations` flags, respectively. The raw data will be 
written to the `data/` directory.

## Viewing Results

To display the results as an ASCII table, run the following command.

```bash
./savina.py view
```

If you ran the evaluation multiple times, the command lets you choose which 
"run" to look at. 

## Full Evaluation

If you have time, run the "full" evaluation. It runs each benchmark for 20 
iterations across 6 JVM invocations and takes about 10 hours. 
Run it with the following command:

```bash
./savina.py full
```

You can customize the number of iterations and JVM invocations with the
`--iter` and `--invocations` flags, respectively.

# Project Overview

Benchmarks are located in `/src/main/scala/edu/rice/habanero/benchmarks`. Each
benchmark from the original Savina repository has been ported to use the 
Pekko/UIGC API.

Configuration files for each benchmark are located in 
`src/main/java/edu/rice/habanero/benchmarks`.