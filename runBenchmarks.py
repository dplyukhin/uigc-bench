import subprocess
import os
import time


def run_benchmark(reqs_per_second, delta_graph_size):
    filename = f"workers-rps-{reqs_per_second}-dgs-{delta_graph_size}"

    # Add JFR options to SBT opts, saving the old value to be restored later.
    original_sbt_opts = os.environ.get("SBT_OPTS", "")

    with (open(f'{filename}.log', 'w') as log):

        processes = []
        for role in ["orchestrator", "manager1", "manager2"]:
            os.environ["SBT_OPTS"] = original_sbt_opts + \
              f" -XX:StartFlightRecording=filename={filename}-{role}.jfr,dumponexit=true"

            print(f"Starting {role}")
            process = subprocess.Popen(
                ["sbt", "-Duigc.crgc.num-nodes=3",
                 f"-Duigc.crgc.delta-graph-size={delta_graph_size}",
                 f"-Drandom-workers.reqs-per-second={reqs_per_second}",
                 f"runMain randomworkers.RandomWorkers {role} 0.0.0.0 0.0.0.0"],
                stdout=log
            )
            processes.append(process)
            time.sleep(5)

        # Wait for all processes to terminate
        for process in processes:
            process.wait()

    # Restore the original SBT_OPTS for the next iteration
    os.environ["SBT_OPTS"] = original_sbt_opts


def run_benchmarks():
    for reqs_per_second in [100, 200, 300, 400, 500]:
        for delta_graph_size in [64, 128, 256, 512, 1024]:
            run_benchmark(reqs_per_second, delta_graph_size)


if __name__ == '__main__':
    run_benchmarks()
