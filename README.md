# UIGC Bench

This is a distributed benchmark suite for the [UIGC](https://github.com/dplyukhin/uigc) library.
It contains the following benchmarks: 

- `randomworkers.RandomWorkers`: A tunable, randomized benchmark with three ActorSystems. 
  Each ActorSystem has a Manager, which processes a stream of requests. Requests are handled 
  by randomly spawning Workers, sending work to Workers, and passing references.
  - *Roles:* `orchestrator`, `manager1`, `manager2`

Benchmarks can be configured by editing the config files in `src/main/resources/`.

## Running the benchmarks

To run a benchmark, each role of the benchmark needs to be started independently.
Each benchmark takes three arguments:

1. `role`: The role this node will play in the benchmark. 
2. `hostname`: The canonical hostname of this node.
3. `leaderhostname`: The canonical hostname of the orchestrator.

For example, the `RandomWorkers` benchmark has three roles;
`orchestrator`, `manager1`, `manager2`. 
To run the benchmark locally, run the following in three different terminals:
```bash
# orchstrator
sbt "runMain randomworkers.RandomWorkers orchestrator 0.0.0.0 0.0.0.0"
```
```bash
# manager1
sbt "runMain randomworkers.RandomWorkers manager1 0.0.0.0 0.0.0.0"
```
```bash
# manager2
sbt "runMain randomworkers.RandomWorkers manager2 0.0.0.0 0.0.0.0"
```

The duration of each iteration (excluding warmups) will be logged to
the filename indicated by the `bench.filename` system property. This
property can be configured by running the benchmark with the argument
`-Dbench.filename="path-to-file-here"`, e.g.

```bash
sbt -Dbench.filename="results.txt" "runMain randomworkers.RandomWorkers orchestrator 0.0.0.0 0.0.0.0"
```