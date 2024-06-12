package io.tapdata.connector.tidb.cdc.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReplaceUtilTest {

    @Test
    void replaceAll() {
        Assertions.assertNull(ReplaceUtil.replaceAll(null, "", ""));
        Assertions.assertEquals("222", ReplaceUtil.replaceAll("111", "1", "2"));
    }
}