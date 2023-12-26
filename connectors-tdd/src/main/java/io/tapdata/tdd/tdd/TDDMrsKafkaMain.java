package io.tapdata.tdd.tdd;

import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.tdd.cli.Main;

public class TDDMrsKafkaMain {
    public static void main(String[] args) {
        CommonUtils.setProperty("pdk_external_jar_path", "D:/gittapdata/tapdata-connectors/connectors/dist");
        args = new String[]{
                "test",
                "-c",
                "D:/gittapdata/tapdata-connectors/connectors-tdd/src/main/resources/config/mrskafka.json",
                "D:/gittapdata/tapdata-connectors/connectors/dist/mrs-kafka-connector-v1.0-SNAPSHOT.jar"
        };
        Main.registerCommands().execute(args);
    }
}
