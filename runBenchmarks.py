import subprocess
import os
import time
import sys
import argparse

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


def run_benchmark(reqs_per_second, output_dir, mode):
    filename = f"workers-rps-{reqs_per_second}"

    # Add JFR options to SBT opts, saving the old value to be restored later.
    original_sbt_opts = os.environ.get("SBT_OPTS", "")

    with open(f'{output_dir}/{filename}.log', 'w') as log:

        processes = []
        for role in ["orchestrator", "manager1", "manager2"]:
            jfr_file = f"{output_dir}/{filename}-{role}.jfr"
            jfr_settings = 'profile.jfc'

            os.environ["SBT_OPTS"] = original_sbt_opts + \
              f" -XX:StartFlightRecording=filename={jfr_file},settings={jfr_settings},dumponexit=true"

            print(f"Starting {role}")
            process = subprocess.Popen(
                ["sbt", "-J-Xmx2G", "-Duigc.crgc.num-nodes=3",
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
    for reqs_per_second in [200, 500]:
        run_benchmark(reqs_per_second, output_dir, mode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run benchmarks for random workers.')
    parser.add_argument('output_dir', type=str, help='The output directory for benchmark logs and recordings.')
    parser.add_argument('mode', type=str, choices=['default', 'big-data', 'streaming'], help='The mode to run the benchmarks in.')

    args = parser.parse_args()

    output_dir = args.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    mode = None
    if args.mode == "default":
        mode = default_mode
    elif args.mode == "big-data":
        mode = big_data_mode
    elif args.mode == "streaming":
        mode = streaming_mode

    run_benchmarks(output_dir, mode)
