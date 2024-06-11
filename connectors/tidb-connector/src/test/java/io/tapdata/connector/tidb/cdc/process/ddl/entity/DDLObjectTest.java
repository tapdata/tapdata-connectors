package io.tapdata.connector.tidb.cdc.process.ddl.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

class DDLObjectTest {
    @Test
    void testNormal() {
        DDLObject ddlObject = new DDLObject();
        ddlObject.setQuery("");
        Assertions.assertEquals("", ddlObject.getQuery());
        ddlObject.setTable("");
        Assertions.assertEquals("", ddlObject.getTable());
        ddlObject.setSchema("");
        Assertions.assertEquals("", ddlObject.getSchema());
        ddlObject.setVersion(1);
        Assertions.assertEquals(1, ddlObject.getVersion());
        ddlObject.setTableVersion(1L);
        Assertions.assertEquals(1L, ddlObject.getTableVersion());
        ddlObject.setType("");
        Assertions.assertEquals("", ddlObject.getType());
        ddlObject.setTableColumns(new ArrayList<>());
        Assertions.assertNotNull(ddlObject.getTableColumns());
        ddlObject.setTableColumnsTotal(1);
        Assertions.assertEquals(1, ddlObject.getTableColumnsTotal());
    }
}