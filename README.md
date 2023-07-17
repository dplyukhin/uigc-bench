# Running the distributed benchmarks

The distributed benchmarks can be executed in two ways:

1. *Locally.* To run a benchmark locally, run it with no arguments.
   The orchestrator and each worker will have their own ActorSystems.
2. *Cluster.* Run it with the following arguments:
   1. `role`: The role this node will play in the benchmark. It can 
      be "orchestrator" or the name of one of the workers.
   2. `hostname`: The canonical hostname of this node.
   3. `leaderhostname`: The canonical hostname of the orchestrator.

The duration of each iteration (excluding warmups) will be logged to
the filename indicated by the `bench.filename` system property. This
property can be configured by running the benchmark with the argument
`-Dbench.filename="path-to-file-here"`.