package io.tapdata.connector.postgres.dml;

import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class PostgresWriteRecorderTest {
    Map<String,Object> beforeData;
    Map<String,Object> afterData;
    TapTable tapTable;
    @BeforeEach
    void init(){
        beforeData = new HashMap<>();
        beforeData.put("key1","value1");
        beforeData.put("key2","value2");
        afterData=new HashMap<>();
        afterData.put("afterKey1","afterValue1");
        afterData.put("afterKey2","afterValue2");
        tapTable = new TapTable();
        tapTable.setId("testTableName");
    }
    @DisplayName("test getDeleteSql when containsNull")
    @Test
    void test1() {
        PostgresWriteRecorder pgWrite = mock(PostgresWriteRecorder.class);
        ReflectionTestUtils.setField(pgWrite,"schema","testSchema");
        ReflectionTestUtils.setField(pgWrite,"tapTable",tapTable);
        doCallRealMethod().when(pgWrite).getDeleteSql(beforeData,true);
        String deleteSql = pgWrite.getDeleteSql(beforeData, true);
        String expectedSql="DELETE FROM \u0000testSchema\u0000.\u0000testTableName\u0000 WHERE (\u0000key1\u0000=? OR (\u0000key1\u0000 IS NULL AND ?::text IS NULL)) AND (\u0000key2\u0000=? OR (\u0000key2\u0000 IS NULL AND ?::text IS NULL))";
        Assertions.assertEquals(expectedSql,deleteSql);
    }
    @DisplayName("test getDeleteSql when not containsNull")
    @Test
    void test2() {
        PostgresWriteRecorder pgWrite = mock(PostgresWriteRecorder.class);
        ReflectionTestUtils.setField(pgWrite,"schema","testSchema");
        ReflectionTestUtils.setField(pgWrite,"tapTable",tapTable);
        doCallRealMethod().when(pgWrite).getDeleteSql(beforeData,false);
        String deleteSql = pgWrite.getDeleteSql(beforeData, false);
        String expectedSql="DELETE FROM \u0000testSchema\u0000.\u0000testTableName\u0000 WHERE \u0000key1\u0000=? AND \u0000key2\u0000=?";
        Assertions.assertEquals(expectedSql,deleteSql);
    }
    @DisplayName("test getUpdateSql when not containsNull")
    @Test
    void test3(){
        PostgresWriteRecorder pgWrite = mock(PostgresWriteRecorder.class);
        ReflectionTestUtils.setField(pgWrite,"schema","testSchema");
        ReflectionTestUtils.setField(pgWrite,"tapTable",tapTable);
        doCallRealMethod().when(pgWrite).getUpdateSql(afterData, beforeData, false);
        String updateSql = pgWrite.getUpdateSql(afterData, beforeData, false);
        String expectedSql="UPDATE \u0000testSchema\u0000.\u0000testTableName\u0000 SET \u0000afterKey1\u0000=?, \u0000afterKey2\u0000=? WHERE \u0000key1\u0000=? AND \u0000key2\u0000=?";
        Assertions.assertEquals(expectedSql,updateSql);
    }
    @DisplayName("test getUpdateSql when containsNull")
    @Test
    void test4(){
        PostgresWriteRecorder pgWrite = mock(PostgresWriteRecorder.class);
        ReflectionTestUtils.setField(pgWrite,"schema","testSchema");
        ReflectionTestUtils.setField(pgWrite,"tapTable",tapTable);
        doCallRealMethod().when(pgWrite).getUpdateSql(afterData, beforeData, true);
        String updateSql = pgWrite.getUpdateSql(afterData, beforeData, true);
        String expectedSql="UPDATE \u0000testSchema\u0000.\u0000testTableName\u0000 SET \u0000afterKey1\u0000=?, \u0000afterKey2\u0000=? WHERE (\u0000key1\u0000=? OR (\u0000key1\u0000 IS NULL AND ?::text IS NULL)) AND (\u0000key2\u0000=? OR (\u0000key2\u0000 IS NULL AND ?::text IS NULL))";
        Assertions.assertEquals(expectedSql,updateSql);
    }
}
