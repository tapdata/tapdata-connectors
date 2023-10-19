package io.tapdata.tdd.tdd;

import io.tapdata.tdd.cli.Main;
import io.tapdata.pdk.core.utils.CommonUtils;

/**
 * @author samuel
 * @Description
 * @create 2022-04-27 19:37
 **/
public class TDDHive1Main {
	public static void main(String... args) {
		CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
		args = new String[]{
//                "test", "-c", "B:\\code\\tapdata\\idaas-tdd\\tapdata-tdd-cli\\src\\main\\resources\\config\\aerospike.json",
//                "test", "-c", "B:\\code\\tapdata\\idaas-tdd\\tapdata-tdd-cli\\src\\main\\resources\\config\\doris.json",
//                "test", "-c", "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd/tapdata-tdd-cli/src/main/resources/config/doris.json",
//				"test", "-c", "connectors-tdd/src/main/resources/config/clickhouse.json",
				"test", "-c", "connectors-tdd/src/main/resources/config/hive1.json",
//                "-t", "io.tapdata.tdd.tdd.tests.target.CreateTableTest",
                "-t", "io.tapdata.tdd.tdd.tests.basic.BasicTest",
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
//				"-m", "/Users/samuel/apache-maven-3.6.1",
//				"connectors/clickhouse-connector",
//				"connectors/hive1-connector",
				"D:\\workspace\\dev\\daasv2.0\\daasv2.8\\tapdata\\connectors\\dist\\hive1-connector-v1.0-SNAPSHOT.jar",
//				"D:\\workspace\\dev\\daasv2.0\\daasv2.8\\tapdata\\connectors\\dist\\clickhouse-connector-v1.0-SNAPSHOT.jar",
//				"D:\\workspace\\dev\\daasv2.0\\daasv2.7\\tapdata\\connectors\\clickhouse-connector\\target\\clickhouse-connector-v1.0-SNAPSHOT.jar",
		};

		Main.registerCommands().execute(args);
	}
}
