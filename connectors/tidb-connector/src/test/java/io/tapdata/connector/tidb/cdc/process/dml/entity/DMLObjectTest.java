package io.tapdata.connector.tidb.cdc.process.dml.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

class DMLObjectTest {

    @Test
    void testNormal() {
        DMLObject dmlObject = new DMLObject();
        dmlObject.setId(0L);
        Assertions.assertEquals(0L, dmlObject.getId());
        dmlObject.setDatabase("");
        Assertions.assertEquals("", dmlObject.getDatabase());
        dmlObject.setTable("");
        Assertions.assertEquals("", dmlObject.getTable());
        dmlObject.setPkNames(new ArrayList<>());
        Assertions.assertNotNull(dmlObject.getPkNames());
        dmlObject.setDdl(true);
        Assertions.assertTrue(dmlObject.getDdl());
        dmlObject.setType("");
        Assertions.assertEquals("", dmlObject.getType());
        dmlObject.setMysqlType(new HashMap<>());
        Assertions.assertNotNull(dmlObject.getMysqlType());
        dmlObject.setEs(0L);
        Assertions.assertEquals(0L, dmlObject.getEs());
        dmlObject.setTs(0L);
        Assertions.assertEquals(0L, dmlObject.getTs());
        dmlObject.setSql("");
        Assertions.assertEquals("", dmlObject.getSql());
        dmlObject.setSqlType(new HashMap<>());
        Assertions.assertNotNull(dmlObject.getSqlType());
        dmlObject.setOld(new ArrayList<>());
        Assertions.assertNotNull(dmlObject.getOld());
        dmlObject.setData(new ArrayList<>());
        Assertions.assertNotNull(dmlObject.getData());
        dmlObject.setTableColumnInfo(new HashMap<>());
        Assertions.assertNotNull(dmlObject.getTableColumnInfo());
    }
}