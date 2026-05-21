package io.tapdata.connector.paimon;

import io.tapdata.entity.schema.value.DateTime;
import org.apache.paimon.data.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test DateTime to Paimon Timestamp conversion, especially for dates before 1970
 */
public class DateTimeConversionTest {

    /**
     * Simulate the conversion logic from PaimonService.convertValueToPaimonType
     */
    private Timestamp convertDateTimeToTimestamp(DateTime dateTime) {
        long epochSecond = dateTime.getSeconds();
        int nanoSecond = dateTime.getNano();
        
        // Convert to milliseconds and nanos-of-millisecond
        long millisecond = epochSecond * 1000L + nanoSecond / 1_000_000;
        int nanoOfMillisecond = nanoSecond % 1_000_000;
        
        // Ensure nanoOfMillisecond is always positive (0-999,999)
        if (nanoOfMillisecond < 0) {
            millisecond -= 1;
            nanoOfMillisecond += 1_000_000;
        }
        
        return Timestamp.fromEpochMillis(millisecond, nanoOfMillisecond);
    }

    @Test
    public void testDateAfter1970() {
        // Test date: 2024-01-01 12:00:00.123456789
        // 123456789 nanos = 123 millis + 456789 nanos-of-millisecond
        LocalDateTime ldt = LocalDateTime.of(2024, 1, 1, 12, 0, 0, 123456789);
        Instant instant = ldt.toInstant(ZoneOffset.UTC);

        DateTime dateTime = new DateTime(instant);
        Timestamp timestamp = convertDateTimeToTimestamp(dateTime);

        assertNotNull(timestamp);
        assertTrue(timestamp.getMillisecond() > 0);
        assertEquals(456789, timestamp.getNanoOfMillisecond());
    }

    @Test
    public void testDateBefore1970() {
        // Test date: 1960-01-01 12:00:00.123456789
        // 123456789 nanos = 123 millis + 456789 nanos-of-millisecond
        LocalDateTime ldt = LocalDateTime.of(1960, 1, 1, 12, 0, 0, 123456789);
        Instant instant = ldt.toInstant(ZoneOffset.UTC);

        DateTime dateTime = new DateTime(instant);
        Timestamp timestamp = convertDateTimeToTimestamp(dateTime);

        assertNotNull(timestamp);
        assertTrue(timestamp.getMillisecond() < 0, "Milliseconds should be negative for dates before 1970");
        assertTrue(timestamp.getNanoOfMillisecond() >= 0 && timestamp.getNanoOfMillisecond() <= 999_999,
                   "NanoOfMillisecond should be in valid range [0, 999999]");
        assertEquals(456789, timestamp.getNanoOfMillisecond());
    }

    @Test
    public void testDateBefore1970WithoutNanos() {
        // Test date: 1950-06-15 00:00:00.000000000
        LocalDateTime ldt = LocalDateTime.of(1950, 6, 15, 0, 0, 0, 0);
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        
        DateTime dateTime = new DateTime(instant);
        Timestamp timestamp = convertDateTimeToTimestamp(dateTime);
        
        assertNotNull(timestamp);
        assertTrue(timestamp.getMillisecond() < 0);
        assertEquals(0, timestamp.getNanoOfMillisecond());
    }

    @Test
    public void testEpochDate() {
        // Test date: 1970-01-01 00:00:00.000000000
        LocalDateTime ldt = LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0);
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        
        DateTime dateTime = new DateTime(instant);
        Timestamp timestamp = convertDateTimeToTimestamp(dateTime);
        
        assertNotNull(timestamp);
        assertEquals(0, timestamp.getMillisecond());
        assertEquals(0, timestamp.getNanoOfMillisecond());
    }

    @Test
    public void testVeryOldDate() {
        // Test date: 1900-01-01 00:00:00.000000000
        LocalDateTime ldt = LocalDateTime.of(1900, 1, 1, 0, 0, 0, 0);
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        
        DateTime dateTime = new DateTime(instant);
        Timestamp timestamp = convertDateTimeToTimestamp(dateTime);
        
        assertNotNull(timestamp);
        assertTrue(timestamp.getMillisecond() < 0);
        assertEquals(0, timestamp.getNanoOfMillisecond());
        
        // Verify round-trip conversion
        LocalDateTime converted = timestamp.toLocalDateTime();
        assertEquals(ldt, converted);
    }

    @Test
    public void testDateBefore1970WithMaxNanos() {
        // Test date: 1960-01-01 12:00:00.999999999
        // 999999999 nanos = 999 millis + 999999 nanos-of-millisecond
        LocalDateTime ldt = LocalDateTime.of(1960, 1, 1, 12, 0, 0, 999999999);
        Instant instant = ldt.toInstant(ZoneOffset.UTC);

        DateTime dateTime = new DateTime(instant);
        Timestamp timestamp = convertDateTimeToTimestamp(dateTime);

        assertNotNull(timestamp);
        assertTrue(timestamp.getMillisecond() < 0);
        assertTrue(timestamp.getNanoOfMillisecond() >= 0 && timestamp.getNanoOfMillisecond() <= 999_999);
        assertEquals(999999, timestamp.getNanoOfMillisecond());
    }
}

