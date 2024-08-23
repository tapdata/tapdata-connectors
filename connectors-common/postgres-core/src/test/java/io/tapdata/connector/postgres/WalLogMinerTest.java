package io.tapdata.connector.postgres;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WalLogMinerTest {

    @Test
    void testParseTimestamp(){
        String timestamp = "2022-02-22 14:33:00.1234";
        Timestamp.valueOf(timestamp);
    }

    @Test
    void testSchemaTableMapSize() {
        Map<String, List<String>> schemaTableMap = new HashMap<>();
        schemaTableMap.put("schema1", Lists.newArrayList("table1", "table2"));
        schemaTableMap.put("schema2", Lists.newArrayList("table3", "table4"));
        schemaTableMap.put("schema3", Lists.newArrayList("table5", "table6", "table7"));
        Assertions.assertEquals(schemaTableMap.entrySet().stream().reduce(0, (a, b) -> a + b.getValue().size(), Integer::sum), 7);
    }
}
