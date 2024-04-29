package io.tapdata.mongodb.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MongoCdcOffsetTest {
    @Test
    void testNormal() {
        MongoCdcOffset o = new MongoCdcOffset();
        Assertions.assertNull(o.getCdcOffset());
        Assertions.assertNull(o.getOpLogOffset());
    }
    @Test
    void testSet() {
        MongoCdcOffset o = new MongoCdcOffset();
        o.setCdcOffset(0);
        o.setOpLogOffset(0);
        Assertions.assertEquals(0, o.getCdcOffset());
        Assertions.assertEquals(0, o.getOpLogOffset());
    }
    @Test
    void testNew() {
        MongoCdcOffset o = new MongoCdcOffset(0, 0);
        Assertions.assertEquals(0, o.getCdcOffset());
        Assertions.assertEquals(0, o.getOpLogOffset());
    }
}