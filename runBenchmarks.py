#!/bin/bash

benchmarks = {
    "apsp.ApspAkkaGCActorBenchmark": [100, 200, 300, 400, 500],
    "astar.GuidedSearchAkkaGCActorBenchmark": [10, 20, 30, 40, 50],# -g
    #"big.BigAkkaGCActorBenchmark", 
    #"chameneos.ChameneosAkkaGCActorBenchmark", 
    "count.CountingAkkaGCActorBenchmark": [1000000, 2000000, 3000000, 4000000, 5000000, 6000000],
    "fib.FibonacciAkkaGCActorBenchmark": [22, 23, 24, 25, 26], 
    #"fjcreate.ForkJoinAkkaGCActorBenchmark", 
    #"fjthrput.ThroughputAkkaGCActorBenchmark", 
    "nqueenk.NQueensAkkaGCActorBenchmark": [10, 11, 12, 13, 14, 15],
    #"pingpong.PingPongAkkaGCActorBenchmark", 
    "quicksort.QuickSortAkkaGCActorBenchmark": [500000, 1000000, 1500000, 2000000, 2500000],
    "radixsort.RadixSortAkkaGCActorBenchmark": [50000, 60000, 70000, 80000, 90000, 100000],
    "recmatmul.MatMulAkkaGCActorBenchmark": [1024, 512, 256, 128, 64],
    #"threadring.ThreadRingAkkaGCActorBenchmark"
}

opts = {
    "apsp.ApspAkkaGCActorBenchmark": "-n",
    "astar.GuidedSearchAkkaGCActorBenchmark": "-g",
    #"big.BigAkkaGCActorBenchmark", 
    #"chameneos.ChameneosAkkaGCActorBenchmark", 
    "count.CountingAkkaGCActorBenchmark": "-n",
    "fib.FibonacciAkkaGCActorBenchmark": "-n",
    #"fjcreate.ForkJoinAkkaGCActorBenchmark", 
    #"fjthrput.ThroughputAkkaGCActorBenchmark", 
    "nqueenk.NQueensAkkaGCActorBenchmark": "-n",
    #"pingpong.PingPongAkkaGCActorBenchmark", 
    "quicksort.QuickSortAkkaGCActorBenchmark": "-n",
    "radixsort.RadixSortAkkaGCActorBenchmark": "-n",
    "recmatmul.MatMulAkkaGCActorBenchmark": "-n",
    #"threadring.ThreadRingAkkaGCActorBenchmark"
}
alt_gcs = ["NoProtocol"]#, "wrc"]
styles = ["on-block", "wave"]
iter=30

import subprocess
for benchmark in benchmarks.keys():
    opt = opts[benchmark]
    for param in benchmarks[benchmark]:

        for gc in alt_gcs:
            filename = f"{benchmark}-n{param}-{gc}.csv"
            subprocess.run(["sbt", f"-Duigc.protocol={gc}", f'"runMain {benchmark} -iter {iter} -filename {filename} {opt} {param}"'])

        for style in styles:
            filename = f"{benchmark}-n{param}-Monotone-{style}.csv"
            subprocess.run(["sbt", f"-Dgc.crgc.collection-style={style}", f"-Duigc.protocol=Monotone", f'"runMain {benchmark} -iter {iter} -filename {filename} {opt} {param}"'])

