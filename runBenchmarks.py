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

acyclic_mode = [
    #"-Drandom-workers.reqs-per-second"
    "-Drandom-workers.total-queries=500",
    "-Drandom-workers.max-acqs-per-msg=0",
    "-Drandom-workers.mgr-probabilities.local-acquaint=0",
    #"-Drandom-workers.max-sends-per-turn"
    #"-Drandom-workers.max-spawns-per-turn"
    #"-Drandom-workers.max-deactivated-per-turn"
    #"-Drandom-workers.manager-max-acquaintances"
    #"-Drandom-workers.total-queries"
    #"-Drandom-workers.query-times-file"
    #"-Drandom-workers.mgr-probabilities.spawn"
    #"-Drandom-workers.mgr-probabilities.local-send"
    #"-Drandom-workers.mgr-probabilities.deactivate"
    #"-Drandom-workers.mgr-probabilities.deactivate-all"
    #"-Drandom-workers.mgr-probabilities.query"
    "-Drandom-workers.wrk-probabilities.acquaint=0",
    #"-Drandom-workers.wrk-probabilities.spawn"
    #"-Drandom-workers.wrk-probabilities.send"
    #"-Drandom-workers.wrk-probabilities.deactivate"
    #"-Drandom-workers.wrk-probabilities.deactivate-all"
]


def run_local(reqs_per_second, output_dir, mode):
    jvm_gc_frequency = 100
    filename = f"workers-rps-{reqs_per_second}"

    # Add JFR options to SBT opts, saving the old value to be restored later.
    original_sbt_opts = os.environ.get("SBT_OPTS", "")

    for jvm_gc_frequency in [100, 1000, 10000]:
        for gc_type in ["crgc", "mac", "manual"]:
            with open(f'{output_dir}/{filename}.log', 'w') as log:

                jfr_file = f"{output_dir}/{filename}.jfr"
                jfr_settings = 'profile.jfc'

                os.environ["SBT_OPTS"] = original_sbt_opts + \
                    f" -XX:StartFlightRecording=filename={jfr_file},settings={jfr_settings},dumponexit=true"

                print(f"Starting orchestrator")
                process = subprocess.Popen(
                    ["sbt", "-J-Xms16G", "-J-Xmx16G", "-J-XX:+UseZGC", "-Duigc.crgc.num-nodes=1", f"-Duigc.engine={gc_type}",
                        f"-Drandom-workers.life-times-file=life-times-{gc_type}-f{jvm_gc_frequency}.csv",
                        f"-Drandom-workers.jvm-gc-frequency={jvm_gc_frequency}",
                        f"-Drandom-workers.reqs-per-second={reqs_per_second}"] +
                    mode +
                    [f"runMain randomworkers.RandomWorkers orchestrator 0.0.0.0 0.0.0.0"],
                    #stdout=log
                )
                # Wait for the orchestrator to terminate
                process.wait()

            # Restore the original SBT_OPTS for the next iteration
            os.environ["SBT_OPTS"] = original_sbt_opts


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
                ["sbt", "-Xms16G", "-J-Xmx16G", "-J-XX:+UseZGC", "-Duigc.crgc.num-nodes=3",
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
    parser.add_argument('mode', type=str, choices=['default', 'big-data', 'streaming', 'local-acyclic'], help='The mode to run the benchmarks in.')

    args = parser.parse_args()

    output_dir = args.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    mode = None

    if args.mode == "local-acyclic":
        mode = acyclic_mode

        run_local(200, output_dir, mode)

    else:
        if args.mode == "default":
            mode = default_mode
        elif args.mode == "big-data":
            mode = big_data_mode
        elif args.mode == "streaming":
            mode = streaming_mode

        run_benchmarks(output_dir, mode)
