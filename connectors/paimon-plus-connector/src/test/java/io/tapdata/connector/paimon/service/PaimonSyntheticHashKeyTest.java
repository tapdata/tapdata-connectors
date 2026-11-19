package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import org.apache.paimon.data.Timestamp;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaimonSyntheticHashKeyTest {

    @Test
    void persistedV2DigestMustKeepItsFieldBoundariesAndKeyOrder() {
        List<String> keys = Arrays.asList("k1", "k2", "k3", "k4", "k5", "k6");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("k6", "x:y");
        row.put("k5", "[x]");
        row.put("k4", "四");
        row.put("k3", "");
        row.put("k2", "c");
        row.put("k1", "a,b");
        String first = PaimonSyntheticHashKey.encode(keys, row);
        assertEquals("063af52640e02b1406cc11e3ec545af2", first);
        row.put("k1", "a");
        row.put("k2", "b,c");
        assertNotEquals(first, PaimonSyntheticHashKey.encode(keys, row));
    }

    @Test
    void emptyValuesAndTypesMustHaveDistinctEncodings() {
        List<Object> values = Arrays.asList(null, "", new byte[0], Collections.emptyList(),
                Collections.emptyMap(), "1", 1, new byte[] {'1'}, "true", true);
        HashSet<String> hashes = new HashSet<>();
        for (Object value : values) {
            hashes.add(hash(value));
        }
        assertEquals(values.size(), hashes.size());
        assertThrows(PaimonFatalWriteException.class, () ->
                PaimonSyntheticHashKey.encode(Collections.singletonList("id"), Collections.emptyMap()));
    }

    @Test
    void nestedValuesMustBeFramedAndIndependentOfMapOrSetIterationOrder() {
        assertNotEquals(hash(Arrays.asList("a,b", "c")), hash(Arrays.asList("a", "b,c")));
        assertNotEquals(hash(Collections.singletonMap("a:b", "c")), hash(Collections.singletonMap("a", "b:c")));
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("set", new LinkedHashSet<>(Arrays.asList("b", "a")));
        first.put("array", new int[] {2, 1});
        Map<String, Object> reordered = new LinkedHashMap<>();
        reordered.put("array", Arrays.asList(2, 1));
        reordered.put("set", new LinkedHashSet<>(Arrays.asList("a", "b")));
        assertEquals(hash(first), hash(reordered));
        assertNotEquals(hash(Arrays.asList("a", "b")), hash(Arrays.asList("b", "a")));
    }

    @Test
    void equivalentNumericAndTimestampRepresentationsMustAddressTheSameRow() {
        assertEquals(hash(1), hash(1L));
        assertEquals(hash(1), hash(new BigDecimal("1.00")));
        Instant instant = Instant.parse("2026-01-01T00:00:00.123456789Z");
        assertEquals(hash(java.sql.Timestamp.from(instant)), hash(Timestamp.fromEpochMillis(instant.toEpochMilli(), 456789)));
        assertNotEquals(hash(java.sql.Timestamp.from(instant)), hash(java.sql.Timestamp.from(instant.plusNanos(1))));
    }

    @Test
    void invalidNumericValuesMustNotLeakIntoErrors() {
        Number value = new BigDecimal("1") {
            @Override
            public String toString() {
                throw new IllegalStateException("primary-key-secret-sentinel");
            }
        };
        PaimonFatalWriteException failure = assertThrows(PaimonFatalWriteException.class, () -> hash(value));
        assertFalse(failure.toString().contains("primary-key-secret-sentinel"));
    }

    private static String hash(Object value) {
        return PaimonSyntheticHashKey.encode(Collections.singletonList("id"), Collections.singletonMap("id", value));
    }
}
