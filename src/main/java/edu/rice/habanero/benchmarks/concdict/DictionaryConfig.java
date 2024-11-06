package edu.rice.habanero.benchmarks.concdict;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class DictionaryConfig {

    protected static int NUM_ENTITIES = 20;
    protected static int NUM_MSGS_PER_WORKER = 10_000;
    protected static int WRITE_PERCENTAGE = 10;

    protected static int DATA_LIMIT = Integer.MAX_VALUE / 4_096;
    protected static Map<Integer, Integer> DATA_MAP = new HashMap<>(DATA_LIMIT);

    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-e":
                    i += 1;
                    NUM_ENTITIES = Integer.parseInt(args[i]);
                    break;
                case "-m":
                    i += 1;
                    NUM_MSGS_PER_WORKER = Integer.parseInt(args[i]);
                    break;
                case "-w":
                    i += 1;
                    WRITE_PERCENTAGE = Integer.parseInt(args[i]);
                    break;
                case "debug":
                case "verbose":
                    debug = true;
                    break;
            }
            i += 1;
        }

        for (int k = 0; k < DATA_LIMIT; k++) {
            DATA_MAP.put(k, k);
        }
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "Num Entities", NUM_ENTITIES);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Message/Worker", NUM_MSGS_PER_WORKER);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Write Percent", WRITE_PERCENTAGE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }
}
