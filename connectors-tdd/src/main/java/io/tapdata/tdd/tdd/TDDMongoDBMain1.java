package io.tapdata.tdd.tdd;

import io.tapdata.tdd.cli.Main;
import io.tapdata.pdk.core.utils.CommonUtils;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 * @author aplomb
 */
public class TDDMongoDBMain1 {
    //
    public static void main(String... args) {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
//                "test", "-c", "B:\\code\\tapdata\\idaas-tdd\\tapdata-tdd-cli\\src\\main\\resources\\config\\aerospike.json",
//                "test", "-c", "B:\\code\\tapdata\\idaas-tdd\\tapdata-tdd-cli\\src\\main\\resources\\config\\doris.json",
//                "test", "-c", "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/tapdata-tdd-cli/src/main/resources/config/doris.json",
//                "test", "-c", "connectors-tdd/src/main/resources/config/mongodb.json",
                "test", "-c", "connectors-tdd/src/main/resources/config/mongodb.json",
//                "-i", "tapdata-api",
//                "-i", "tapdata-tdd-api",
//                "-i", "connectors/connector-core",
//                "-m", "/usr/local/Cellar/maven/3.6.2/libexec",
//                "-t", "io.tapdata.tdd.tdd.tests.target.CreateTableTest",
//                "-t", "io.tapdata.tdd.tdd.tests.basic.BasicTest",
//                "-t", "io.tapdata.tdd.tdd.tests.target.DMLTest",
//                "-t", "io.tapdata.tdd.tdd.tests.source.BatchReadTest",
//                "-t", "io.tapdata.tdd.tdd.tests.source.StreamReadTest",
//                "B:\\code\\tapdata\\idaas-tdd\\connectors\\aerospike-connector\\target\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/dist/doris-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/dist/empty-connector-v1.1-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/dist/file-connector-v1.0-SNAPSHOT.jar",

                "./connectors/dist/mongodb-connector-v1.0-SNAPSHOT.jar",
        };

		Main.registerCommands().execute(args);
    }
}
