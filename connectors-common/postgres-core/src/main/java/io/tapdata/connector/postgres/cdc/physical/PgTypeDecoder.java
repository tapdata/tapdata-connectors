package io.tapdata.connector.postgres.cdc.physical;

import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapStringValue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Converts the raw on-disk bytes of a single (already de-TOASTed) attribute
 * value into a Java object, selected by the column's type oid. Unknown types
 * fall back to a UTF-8 string or the raw bytes so that decoding degrades
 * gracefully rather than failing.
 *
 * @author Jarad
 */
public final class PgTypeDecoder {

    private PgTypeDecoder() {
    }

    public static final long BOOL = 16, BYTEA = 17, CHAR = 18, NAME = 19, INT8 = 20, INT2 = 21,
            INT4 = 23, TEXT = 25, OID = 26, JSON = 114, XML = 142, FLOAT4 = 700, FLOAT8 = 701,
            BPCHAR = 1042, VARCHAR = 1043, DATE = 1082, TIME = 1083, TIMESTAMP = 1114,
            TIMESTAMPTZ = 1184, TIMETZ = 1266, INTERVAL = 1186, MONEY = 790, BIT = 1560, VARBIT = 1562, NUMERIC = 1700,
            UUID_OID = 2950, JSONB = 3802;

    private static final LocalDate PG_EPOCH = LocalDate.of(2000, 1, 1);
    private static final LocalDateTime PG_EPOCH_TS = LocalDateTime.of(2000, 1, 1, 0, 0);

    public static Object decode(long oid, byte[] v) {
        if (v == null) {
            return null;
        }
        if (oid == BOOL) {
            return v.length > 0 && v[0] != 0;
        } else if (oid == INT2) {
            return (int) (short) le16(v);
        } else if (oid == INT4) {
            return (int) le32(v);
        } else if (oid == INT8) {
            return le64(v);
        } else if (oid == OID) {
            return le32(v) & 0xFFFFFFFFL;
        } else if (oid == FLOAT4) {
            return Float.intBitsToFloat((int) le32(v));
        } else if (oid == FLOAT8) {
            return Double.longBitsToDouble(le64(v));
        } else if (oid == BIT || oid == VARBIT) {
            return decodeBit(v);
        } else if (oid == INTERVAL) {
            return decodeInterval(v);
        } else if (oid == MONEY) {
            return decodeMoney(v);
        } else if (oid == NUMERIC) {
            return decodeNumeric(v);
        } else if (oid == DATE) {
            return PG_EPOCH.plusDays(le32(v));
        } else if (oid == TIME) {
            return LocalTime.ofNanoOfDay(le64(v) * 1000L);
        } else if (oid == TIMESTAMP) {
            return PG_EPOCH_TS.plus(le64(v) * 1000L, java.time.temporal.ChronoUnit.NANOS);
        } else if (oid == TIMESTAMPTZ) {
            long micros = le64(v);
            return PG_EPOCH_TS.plus(micros * 1000L, java.time.temporal.ChronoUnit.NANOS).toInstant(ZoneOffset.UTC);
        } else if (oid == TIMETZ) {
            return decodeTimeTz(v);
        } else if (oid == UUID_OID) {
            return decodeUuid(v);
        } else if (oid == BYTEA) {
            return v;
        } else if (oid == NAME) {
            // "name" is a fixed NAMEDATALEN (64-byte) buffer holding a
            // null-terminated string; the bytes past the terminator are NUL
            // padding and must be dropped (otherwise field names carry \u0000).
            return cString(v);
        } else if (oid == CHAR || oid == TEXT || oid == VARCHAR || oid == BPCHAR
                || oid == JSON || oid == JSONB || oid == XML) {
            return jsonbStrip(oid, new String(v, StandardCharsets.UTF_8));
        }
        return new String(v, StandardCharsets.UTF_8);
    }

    /**
     * Decode PostgreSQL on-disk bit/varbit representation.
     *
     * On-disk layout (after varlena header is stripped):
     *   int32  bit_len     — number of valid bits (little-endian)
     *   bits8  bit_dat[]   — packed bits, MSB-first, zero-padded in last byte
     *
     * bit(1)='1'   → [0x01,0x00,0x00,0x00, 0x80] → true (Boolean)
     * bit(2)='10'  → [0x02,0x00,0x00,0x00, 0x80] → "10"
     * bit(2)='00'  → [0x02,0x00,0x00,0x00, 0x00] → "00"
     * bit(2)='01'  → [0x02,0x00,0x00,0x00, 0x40] → "01"
     */
    private static Object decodeBit(byte[] v) {
        if (v == null || v.length < 4) {
            return v;
        }
        int bitLen = (int) le32(v);
        int dataStart = 4;
        // bit(1): return boolean, matching AbstractWalLogMiner.parseType() behavior
        if (bitLen == 1) {
            return new TapBooleanValue((v[dataStart] & 0x80) != 0);
        }
        // bit(n) n > 1: unpack each bit to a '0'/'1' character string
        StringBuilder sb = new StringBuilder(bitLen);
        for (int byteIdx = dataStart; byteIdx < v.length && sb.length() < bitLen; byteIdx++) {
            int b = v[byteIdx] & 0xFF;
            for (int bitIdx = 7; bitIdx >= 0 && sb.length() < bitLen; bitIdx--) {
                sb.append(((b >> bitIdx) & 1) == 1 ? '1' : '0');
            }
        }
        return new TapStringValue(sb.toString());
    }

    /**
     * Decode PostgreSQL on-disk interval representation into ISO 8601 duration.
     *
     * On-disk layout (16 bytes, little-endian):
     *   int64 time_us  — microseconds
     *   int32 day      — days
     *   int32 month    — months
     *
     * '1 year 1 mon 1 day 1 hour 2 mins 4.11 secs' yields:
     *   month=13, day=1, time_us=3724110000 → "P1Y1M1DT1H2M4.11S"
     */
    private static String decodeInterval(byte[] v) {
        if (v == null || v.length < 16) {
            return null;
        }
        long timeUs = le64(v);                   // bytes[0..7]
        int day = (int) le32(v, 8);              // bytes[8..11]
        int month = (int) le32(v, 12);           // bytes[12..15]

        StringBuilder iso = new StringBuilder();

        if (timeUs == 0 && day == 0 && month == 0) {
            return "PT0S";
        }

        boolean neg = timeUs < 0 || day < 0 || month < 0;
        if (neg) {
            iso.append('-');
            timeUs = -timeUs;
            day = -day;
            month = -month;
        }
        iso.append('P');

        // years + months
        if (month != 0) {
            int years = month / 12;
            int mons = month % 12;
            if (years != 0) {
                iso.append(years).append('Y');
            }
            if (mons != 0) {
                iso.append(mons).append('M');
            }
        }

        // days
        if (day != 0) {
            iso.append(day).append('D');
        }

        // time portion
        if (timeUs != 0) {
            iso.append('T');
            long totalSec = timeUs / 1_000_000;
            long hours = totalSec / 3600;
            long mins = (totalSec % 3600) / 60;
            long secInt = totalSec % 60;
            long fracUs = timeUs % 1_000_000;

            if (hours != 0) {
                iso.append(hours).append('H');
            }
            if (mins != 0) {
                iso.append(mins).append('M');
            }
            if (secInt != 0 || fracUs != 0) {
                // Strip trailing zeros from fractional seconds for clean output
                if (fracUs == 0) {
                    iso.append(secInt).append('S');
                } else {
                    // Build seconds string with fractional part, strip trailing zeros
                    String frac = String.format("%06d", fracUs);
                    // Strip trailing zeros
                    frac = frac.replaceAll("0+$", "");
                    iso.append(secInt).append('.').append(frac).append('S');
                }
            }
        }

        return iso.toString();
    }

    /**
     * Decode PostgreSQL money type (8-byte little-endian int64, value in cents).
     * '1212.09' → 121209 cents → BigDecimal("1212.09")
     */
    private static BigDecimal decodeMoney(byte[] v) {
        if (v == null || v.length < 8) {
            return null;
        }
        long cents = le64(v);
        return BigDecimal.valueOf(cents, 2);
    }

    /**
     * Decode PostgreSQL time-with-time-zone (timetz) on-disk representation.
     *
     * On-disk layout (12 bytes, little-endian, fixed-length):
     *   int64 time_us  — microseconds since midnight
     *   int32 zone     — timezone offset in seconds (east of GMT positive)
     *
     * '12:12:12.000000 +00:00' → ZonedDateTime(1970-01-01T12:12:12Z)
     */
    private static ZonedDateTime decodeTimeTz(byte[] v) {
        if (v == null || v.length < 12) {
            return null;
        }
        long micros = le64(v);           // bytes[0..7]
        int zoneSecs = (int) le32(v, 8); // bytes[8..11]
        LocalTime time = LocalTime.ofNanoOfDay(micros * 1000L);
        ZoneOffset offset = ZoneOffset.ofTotalSeconds(zoneSecs);
        return ZonedDateTime.of(LocalDate.of(1970, 1, 1), time, offset);
    }

    /* Decode a NUL-terminated string out of a fixed-width buffer (PostgreSQL
     * "name"): stop at the first NUL so the trailing pad bytes are excluded. */
    private static String cString(byte[] v) {
        int len = 0;
        while (len < v.length && v[len] != 0) {
            len++;
        }
        return new String(v, 0, len, StandardCharsets.UTF_8);
    }

    private static Object jsonbStrip(long oid, String s) {
        // jsonb stores a 1-byte version prefix before the textual form when not
        // sent in text mode; the de-TOASTed bytes here are already textual, so
        // return as-is. Hook kept for clarity / future binary jsonb handling.
        return s;
    }

    /* on-disk numeric (NumericChoice) header bits, see utils/adt/numeric.c */
    private static final int NUMERIC_SIGN_MASK = 0xC000;
    private static final int NUMERIC_NEG = 0x4000;
    private static final int NUMERIC_SHORT = 0x8000;
    private static final int NUMERIC_SPECIAL = 0xC000;
    private static final int NUMERIC_SHORT_SIGN_MASK = 0x2000;
    private static final int NUMERIC_SHORT_DSCALE_MASK = 0x1F80;
    private static final int NUMERIC_SHORT_DSCALE_SHIFT = 7;
    private static final int NUMERIC_SHORT_WEIGHT_SIGN_MASK = 0x0040;
    private static final int NUMERIC_SHORT_WEIGHT_MASK = 0x003F;
    private static final int NUMERIC_DSCALE_MASK = 0x3FFF;

    /**
     * Decodes a numeric value in PostgreSQL's on-disk ({@code NumericChoice})
     * layout as carried in heap tuples / WAL: a packed 2-byte header (short form)
     * or a 2-byte sign+dscale plus a 2-byte weight (long form), each followed by
     * base-10000 {@code NumericDigit}s. This differs from the binary send/recv
     * wire format, which prefixes four int16s (ndigits, weight, sign, dscale).
     */
    static BigDecimal decodeNumeric(byte[] v) {
        if (v == null || v.length < 2) {
            return null;
        }
        int header = (v[0] & 0xFF) | ((v[1] & 0xFF) << 8);
        if ((header & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL) {
            return null; // NaN / +Inf / -Inf have no BigDecimal representation
        }
        boolean negative;
        int weight;
        int dscale;
        int digitsStart;
        if ((header & NUMERIC_SHORT) != 0) {
            negative = (header & NUMERIC_SHORT_SIGN_MASK) != 0;
            dscale = (header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT;
            weight = header & NUMERIC_SHORT_WEIGHT_MASK;
            if ((header & NUMERIC_SHORT_WEIGHT_SIGN_MASK) != 0) {
                weight |= ~NUMERIC_SHORT_WEIGHT_MASK;   // sign-extend negative weight
            }
            digitsStart = 2;
        } else {
            negative = (header & NUMERIC_SIGN_MASK) == NUMERIC_NEG;
            dscale = header & NUMERIC_DSCALE_MASK;
            weight = (short) ((v[2] & 0xFF) | ((v[3] & 0xFF) << 8));
            digitsStart = 4;
        }
        int ndigits = (v.length - digitsStart) / 2;
        BigDecimal val = BigDecimal.ZERO;
        for (int i = 0; i < ndigits; i++) {
            int off = digitsStart + i * 2;
            int digit = (v[off] & 0xFF) | ((v[off + 1] & 0xFF) << 8);
            val = val.add(BigDecimal.valueOf(digit).scaleByPowerOfTen((weight - i) * 4));
        }
        if (negative) {
            val = val.negate();
        }
        return val.setScale(dscale, RoundingMode.HALF_UP);
    }

    private static String decodeUuid(byte[] v) {
        long msb = 0, lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (v[i] & 0xFFL);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (v[i] & 0xFFL);
        }
        return new UUID(msb, lsb).toString();
    }

    private static int le16(byte[] b) {
        return (b[0] & 0xFF) | ((b[1] & 0xFF) << 8);
    }

    private static long le32(byte[] b) {
        return (b[0] & 0xFFL) | ((b[1] & 0xFFL) << 8) | ((b[2] & 0xFFL) << 16) | ((b[3] & 0xFFL) << 24);
    }

    private static long le32(byte[] b, int offset) {
        return (b[offset] & 0xFFL) | ((b[offset + 1] & 0xFFL) << 8)
                | ((b[offset + 2] & 0xFFL) << 16) | ((b[offset + 3] & 0xFFL) << 24);
    }

    private static long le64(byte[] b) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (b[i] & 0xFFL) << (8 * i);
        }
        return v;
    }
}
