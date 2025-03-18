package edu.rice.habanero.benchmarks.banking;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class BankingConfig {

    protected static int A = 1_000; // num accounts
    protected static int N = 50_000; // num transactions
    protected static double INITIAL_BALANCE = Double.MAX_VALUE / (A * N);
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-a":
                    i += 1;
                    A = Integer.parseInt(args[i]);
                    break;
                case "-n":
                    i += 1;
                    N = Integer.parseInt(args[i]);
                    break;
                case "-debug":
                case "-verbose":
                    debug = true;
                    break;
            }
            i += 1;
        }

        INITIAL_BALANCE = ((Double.MAX_VALUE / (A * N)) / 1_000) * 1_000;
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "A (num accounts)", A);
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num transactions)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Initial Balance", INITIAL_BALANCE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

}
