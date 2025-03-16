[Savina](https://github.com/shamsimam/savina) is an Actor Benchmark Suite, originally developed by Shams Imam and Vivek Sarkar at Rice University.

This repository is a fork of Savina. Benchmarks related to Akka have been ported to Pekko. Benchmarks related to other actor frameworks have been removed.

# Quick Start

You must have `python3` and `numpy` installed. For a quick evaluation, run the script:

```bash
./savina.py quick
```

The full evaluation takes about a day to complete. Run it with the script:

```bash
./savina.py full
```

# Overview

Benchmarks are located in `/src/main/scala/edu/rice/habanero/benchmarks`.

Configuration files for each benchmark are located in `src/main/java/edu/rice/habanero/benchmarks`.