package io.tapdata.mongodb.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

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
    @Nested
    class FromOffsetTest {

        @Test
        void testFromNormalMap() {
            Map<String, Object> map = new HashMap<>();
            MongoCdcOffset mongoCdcOffset = MongoCdcOffset.fromOffset(map);
            Assertions.assertNotNull(mongoCdcOffset);
            Assertions.assertNull(mongoCdcOffset.getOpLogOffset());
            Assertions.assertNotNull(mongoCdcOffset.getCdcOffset());
            Assertions.assertEquals(map, mongoCdcOffset.getCdcOffset());
        }

        @Test
        void testFromOPLogMap1() {
            Map<String, Object> map = new HashMap<>();
            map.put(MongoCdcOffset.MONGO_CDC_OFFSET_FLAG, true);
            MongoCdcOffset mongoCdcOffset = MongoCdcOffset.fromOffset(map);
            Assertions.assertNotNull(mongoCdcOffset);
            Assertions.assertNull(mongoCdcOffset.getOpLogOffset());
            Assertions.assertNull(mongoCdcOffset.getCdcOffset());
        }
        @Test
        void testFromOPLogMap2() {
            Map<String, Object> map = new HashMap<>();
            map.put(MongoCdcOffset.MONGO_CDC_OFFSET_FLAG, new HashMap<>());
            MongoCdcOffset mongoCdcOffset = MongoCdcOffset.fromOffset(map);
            Assertions.assertNotNull(mongoCdcOffset);
            Assertions.assertNull(mongoCdcOffset.getOpLogOffset());
            Assertions.assertNotNull(mongoCdcOffset.getCdcOffset());
            Assertions.assertEquals(map, mongoCdcOffset.getCdcOffset());
        }
    }
}