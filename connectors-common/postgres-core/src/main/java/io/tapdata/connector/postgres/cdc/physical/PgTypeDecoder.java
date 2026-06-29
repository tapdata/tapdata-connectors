package io.tapdata.connector.postgres.cdc.physical;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
            TIMESTAMPTZ = 1184, NUMERIC = 1700, UUID_OID = 2950, JSONB = 3802;

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

    private static long le64(byte[] b) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (b[i] & 0xFFL) << (8 * i);
        }
        return v;
    }
}
