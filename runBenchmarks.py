import subprocess
import os
import time
import sys

default_mode = []

big_data_mode = [
    "-Drandom-workers.max-work-size-in-bytes=5120"
]

streaming_mode = [
    "-Drandom-workers.max-work-size-in-bytes=5120",
    "-Drandom-workers.wrk-probabilities.spawn=0",
    "-Drandom-workers.wrk-probabilities.acquaint=0",
    "-Drandom-workers.mgr-probabilities.spawn=0.01",
    "-Drandom-workers.mgr-probabilities.local-acquaint=0",
    "-Drandom-workers.mgr-probabilities.publish-worker=1",
    "-Drandom-workers.mgr-probabilities.deactivate=0",
    "-Drandom-workers.total-queries=20000",
]


def run_benchmark(reqs_per_second, delta_graph_size, output_dir, mode):
    filename = f"workers-rps-{reqs_per_second}-dgs-{delta_graph_size}"

    # Add JFR options to SBT opts, saving the old value to be restored later.
    original_sbt_opts = os.environ.get("SBT_OPTS", "")

    with open(f'{output_dir}/{filename}.log', 'w') as log:

        processes = []
        for role in ["orchestrator", "manager1", "manager2"]:
            os.environ["SBT_OPTS"] = original_sbt_opts + \
              f" -XX:StartFlightRecording=filename={output_dir}/{filename}-{role}.jfr,dumponexit=true"

            print(f"Starting {role}")
            process = subprocess.Popen(
                ["sbt", "-J-Xmx2G", "-Duigc.crgc.num-nodes=3",
                 f"-Duigc.crgc.delta-graph-size={delta_graph_size}",
                 f"-Drandom-workers.reqs-per-second={reqs_per_second}"] +
                mode +
                [f"runMain randomworkers.RandomWorkers {role} 0.0.0.0 0.0.0.0"],
                stdout=log
            )
            processes.append(process)
            time.sleep(5)

        # Wait for all processes to terminate
        for process in processes:
            process.wait()

    # Restore the original SBT_OPTS for the next iteration
    os.environ["SBT_OPTS"] = original_sbt_opts


def run_benchmarks(output_dir, mode):
    for reqs_per_second in [200, 300, 400, 500]:
        for delta_graph_size in [128, 256, 512, 1024, 2048]:
            run_benchmark(reqs_per_second, delta_graph_size, output_dir, mode)


if __name__ == '__main__':
    # Get the output directory from the command line
    if len(sys.argv) != 3:
        print("Usage: python runBenchmarks.py <output-directory> <mode>")
        sys.exit(1)

    output_dir = sys.argv[1]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    mode = None
    mode_arg = sys.argv[2]
    if mode_arg == "default":
        mode = default_mode
    elif mode_arg == "big-data":
        mode = big_data_mode
    elif mode_arg == "streaming":
        mode = streaming_mode
    else:
        print("Invalid mode. Choose one of: default, big-data, streaming")
        sys.exit(1)

    run_benchmarks(output_dir, mode)
