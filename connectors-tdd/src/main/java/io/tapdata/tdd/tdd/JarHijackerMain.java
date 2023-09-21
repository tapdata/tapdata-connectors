package io.tapdata.tdd.tdd;

import io.tapdata.tdd.cli.Main;

public class JarHijackerMain {
    public static void main(String[] args) {
        args = new String[]{
                "jar",
                "-m", "connectors/mongodb-connector",
                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd-new/dist/mongodb-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-tdd-new/dist/mysql-connector-v1.0-SNAPSHOT.jar"
        };

		Main.registerCommands().execute(args);
    }
}
