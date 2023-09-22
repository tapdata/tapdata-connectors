package io.tapdata.tdd.tdd;

import io.tapdata.tdd.cli.Main;
import io.tapdata.pdk.core.utils.CommonUtils;

/**
 * @author jackin
 * @Description
 * @create 2022-12-15 11:49
 **/
public class TDDTencentDBPostgresMain {
	public static void main(String... args) {
		CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
		args = new String[]{
//                "test", "-c", "B:\\code\\tapdata\\idaas-tdd\\tapdata-tdd-cli\\src\\main\\resources\\config\\aerospike.json",
//                "test", "-c", "B:\\code\\tapdata\\idaas-tdd\\tapdata-tdd-cli\\src\\main\\resources\\config\\doris.json",
//                "test", "-c", "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/tapdata-tdd-cli/src/main/resources/config/doris.json",
				"test",
				"-c",
				"connectors-tdd/src/main/resources/config/tencent-db-postgres.json",
//                "-t", "io.tapdata.tdd.tdd.tests.target.CreateTableTest",
//                "-t", "io.tapdata.tdd.tdd.tests.basic.BasicTest",
//                "-t", "io.tapdata.tdd.tdd.tests.target.DMLTest",
//                "-t", "io.tapdata.tdd.tdd.tests.source.BatchReadTest",
//                "-t", "io.tapdata.tdd.tdd.tests.source.StreamReadTest",
//                "B:\\code\\tapdata\\idaas-tdd\\connectors\\aerospike-connector\\target\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/dist/doris-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/dist/empty-connector-v1.1-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/dist/file-connector-v1.0-SNAPSHOT.jar",
//				"-i", "tapdata-api",
//                "-i", "tapdata-tdd-api",
//                "-i", "connectors/connector-core",
//                "-i", "connectors/mysql/mysql-core",
				"-m", "/Applications/apache-maven-3.8.1",
				"connectors/tencent-db-postgres-connector",
		};

		Main.registerCommands().execute(args);
	}
}
