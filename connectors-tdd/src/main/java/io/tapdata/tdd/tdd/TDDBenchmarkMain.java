package io.tapdata.tdd.tdd;

import io.tapdata.tdd.cli.Main;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class TDDBenchmarkMain {
    //
    public static void main(String... args) {
        args = new String[]{
                "test", "-c", "B:\\code\\tapdata\\idaas-tdd\\tapdata-tdd-cli\\src\\main\\resources\\config\\aerospike.json",
//                "test", "-c", "B:\\code\\tapdata\\idaas-tdd\\tapdata-tdd-cli\\src\\main\\resources\\config\\emptyBenchmark.json",
                "-t", "io.tapdata.tdd.tdd.tests.target.benchmark.BenchmarkTest",
                "B:\\code\\tapdata\\idaas-tdd\\dist\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "B:\\code\\tapdata\\idaas-tdd\\dist\\doris-connector-v1.0-SNAPSHOT.jar",
//                "B:\\code\\tapdata\\idaas-tdd\\dist\\empty-connector-v1.1-SNAPSHOT.jar",

        };

		Main.registerCommands().execute(args);
    }
}
