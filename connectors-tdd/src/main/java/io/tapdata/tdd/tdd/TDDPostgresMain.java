package io.tapdata.tdd.tdd;

import io.tapdata.tdd.cli.Main;
import io.tapdata.pdk.core.utils.CommonUtils;

public class TDDPostgresMain {
    public static void main(String... args) {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
//                "test", "-c", "B:\\code\\tapdata\\idaas-tdd\\tapdata-tdd-cli\\src\\main\\resources\\config\\aerospike.json",
//                "test", "-c", "B:\\code\\tapdata\\idaas-tdd\\tapdata-tdd-cli\\src\\main\\resources\\config\\doris.json",
//                "test", "-c", "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/tapdata-tdd-cli/src/main/resources/config/doris.json",
                "test", "-c", "connectors-tdd/src/main/resources/config/postgres.json",
//                "-t", "io.tapdata.tdd.tdd.tests.source.StreamReadTest",
//                "-t", "io.tapdata.tdd.tdd.tests.source.BatchReadTest",
//                "-t", "io.tapdata.tdd.tdd.tests.target.CreateTableTest",
//                "-t", "io.tapdata.tdd.tdd.tests.target.DMLTest",
//                "B:\\code\\tapdata\\idaas-tdd\\connectors\\aerospike-connector\\target\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/dist/doris-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/dist/empty-connector-v1.1-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/dist/file-connector-v1.0-SNAPSHOT.jar",
				"-m", "/Applications/apache-maven-3.8.1",
                "connectors/postgres-connector",};

		Main.registerCommands().execute(args);
    }
}
