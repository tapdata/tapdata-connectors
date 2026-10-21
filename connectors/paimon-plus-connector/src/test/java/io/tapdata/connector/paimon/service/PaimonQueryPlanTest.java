package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class PaimonQueryPlanTest {
    private static final RowType TYPE = RowType.of(
            new DataType[]{DataTypes.STRING(), DataTypes.DECIMAL(20, 2), DataTypes.INT()},
            new String[]{"region", "amount", "unused"});

    @Test
    void preservesDecimalPrecisionAndNullSemantics() throws Exception {
        PaimonQueryPlan plan = plan("SELECT region, COUNT(*) AS n, COUNT(amount) AS present, SUM(amount) AS total, "
                + "MIN(amount) AS lo, MAX(amount) AS hi, AVG(amount) AS mean "
                + "FROM orders GROUP BY region ORDER BY region DESC NULLS LAST");
        assertArrayEquals(new int[]{0, 1}, plan.projection());
        PaimonQueryPlan.Accumulator accumulator = plan.accumulator(limits());
        accumulator.add(row("A", "99999999999999999.99"));
        accumulator.add(row("A", "0.01"));
        accumulator.add(row("A", null));
        accumulator.add(row(null, null));
        List<Map<String, Object>> result = accumulator.finish();
        assertEquals(2, result.size());
        Map<String, Object> first = result.get(0);
        assertEquals("A", first.get("region"));
        assertEquals(3L, first.get("n"));
        assertEquals(2L, first.get("present"));
        assertEquals(0, new BigDecimal("100000000000000000.00").compareTo((BigDecimal) first.get("total")));
        assertEquals(new BigDecimal("0.01"), first.get("lo"));
        assertEquals(new BigDecimal("99999999999999999.99"), first.get("hi"));
        assertEquals(0, new BigDecimal("50000000000000000").compareTo((BigDecimal) first.get("mean")));
        assertNull(result.get(1).get("region"));
        assertNull(result.get(1).get("total"));
        assertEquals(0L, result.get(1).get("present"));
    }

    @Test
    void emptyInputHasSqlAggregateSemantics() throws Exception {
        PaimonQueryPlan.Accumulator global = plan("SELECT COUNT(*) AS n, SUM(amount) AS s, AVG(amount) AS a FROM orders")
                .accumulator(limits());
        Map<String, Object> result = global.finish().get(0);
        assertEquals(0L, result.get("n"));
        assertNull(result.get("s"));
        assertNull(result.get("a"));
        assertTrue(plan("SELECT region, SUM(amount) FROM orders GROUP BY region").accumulator(limits()).finish().isEmpty());
    }

    @Test
    void limitsNeverReturnPartialGroupsOrRows() throws Exception {
        PaimonQueryPlan plan = plan("SELECT region, SUM(amount) AS s FROM orders GROUP BY region");
        PaimonQueryPlan.Accumulator groups = plan.accumulator(new PaimonQueryLimits(Collections.singletonMap("maxGroups", 1)));
        groups.add(row("A", "1.00"));
        assertTrue(assertThrows(IllegalStateException.class, () -> groups.add(row("B", "2.00"))).getMessage().contains("maxGroups"));
        PaimonQueryPlan.Accumulator results = plan.accumulator(new PaimonQueryLimits(Collections.singletonMap("maxResultRows", 1)));
        results.add(row("A", "1.00"));
        results.add(row("B", "2.00"));
        assertThrows(IllegalStateException.class, results::finish);
    }

    @Test
    void limitsValidateValuesAndCheckMonotonicDeadlineAndInterrupts() throws Exception {
        for (Object invalid : Arrays.asList(-1, 0, "1.5", "9223372036854775808", "oops")) {
            assertThrows(IllegalArgumentException.class,
                    () -> new PaimonQueryLimits(Collections.singletonMap("maxScanRows", invalid)));
        }
        assertThrows(IllegalArgumentException.class,
                () -> new PaimonQueryLimits(Collections.singletonMap("queryTimeoutSeconds", "2147483648")));
        assertTrue(new PaimonQueryLimits(Collections.singletonMap("allowFullScan", "true")).allowFullScan);
        assertFalse(new PaimonQueryLimits(Collections.singletonMap("allowFullScan", "FALSE")).allowFullScan);
        assertThrows(IllegalArgumentException.class,
                () -> new PaimonQueryLimits(Collections.singletonMap("allowFullScan", "yes")));
        AtomicLong clock = new AtomicLong();
        PaimonQueryLimits limits = new PaimonQueryLimits(Collections.singletonMap("queryTimeoutSeconds", 1), clock::get);
        limits.check();
        clock.set(1_000_000_000L);
        assertThrows(SQLTimeoutException.class, limits::check);
        try {
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, limits()::check);
        } finally { Thread.interrupted(); }
    }

    @Test
    void recognizesSafeKeyFiltersAcrossBooleanPredicates() {
        assertTrue(plan("SELECT SUM(amount) FROM orders WHERE region = 'A'")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
        assertTrue(plan("SELECT SUM(amount) FROM orders WHERE region = 'A' OR region = 'B'")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
        assertFalse(plan("SELECT SUM(amount) FROM orders WHERE region = 'A' OR amount > 1")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
        assertTrue(plan("SELECT SUM(amount) FROM orders WHERE region = 'A' AND amount > 1")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
        assertTrue(plan("SELECT SUM(amount) FROM orders WHERE region IS NULL")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
        assertFalse(plan("SELECT SUM(amount) FROM orders WHERE region IS NOT NULL")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
        assertFalse(plan("SELECT SUM(amount) FROM orders WHERE NOT (region = 'A')")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
        assertTrue(plan("SELECT SUM(amount) FROM orders WHERE region IN ('A', 'B')")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
        assertFalse(plan("SELECT SUM(amount) FROM orders WHERE region NOT IN ('A', 'B')")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
        assertFalse(plan("SELECT SUM(amount) FROM orders WHERE amount > 1")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
    }

    @Test
    void groupsMultipleColumnsAndSortsAnAliasedAggregateExpression() throws Exception {
        PaimonQueryPlan.Accumulator accumulator = plan("SELECT region, amount, COUNT(*) AS n "
                + "FROM orders GROUP BY region, amount ORDER BY COUNT(*) DESC, region ASC")
                .accumulator(limits());
        accumulator.add(row("A", "1.00"));
        accumulator.add(row("A", "2.00"));
        accumulator.add(row("A", "1.00"));
        accumulator.add(row("B", "1.00"));
        List<Map<String, Object>> rows = accumulator.finish();
        assertEquals(3, rows.size());
        assertEquals(2L, rows.get(0).get("n"));
        assertEquals(new BigDecimal("1.00"), rows.get(0).get("amount"));
        assertEquals("B", rows.get(2).get("region"));
        PaimonQueryPlan.Accumulator all = plan("SELECT COUNT(ALL amount) AS n FROM orders").accumulator(limits());
        // Only amount is projected for this query.
        all.add(GenericRow.of((Object) null));
        assertEquals(0L, all.finish().get(0).get("n"));
    }

    @Test
    void streamsTwoHundredThousandRowsIntoOneHundredGroups() throws Exception {
        PaimonQueryPlan.Accumulator accumulator = plan("SELECT region, COUNT(*) AS n, SUM(amount) AS s "
                + "FROM orders GROUP BY region ORDER BY region").accumulator(limits());
        GenericRow[] input = new GenericRow[100];
        for (int i = 0; i < input.length; i++) { input[i] = row("region_" + i, "0.01"); }
        for (int i = 0; i < 200_000; i++) { accumulator.add(input[i % input.length]); }
        assertEquals(100, accumulator.groupCount());
        List<Map<String, Object>> result = accumulator.finish();
        assertEquals(100, result.size());
        for (Map<String, Object> group : result) {
            assertEquals(2000L, group.get("n"));
            assertEquals(new BigDecimal("20.00"), group.get("s"));
        }
    }

    @Test
    void timeoutIsOptInAndLegacyLimitsPreserveExistingQueries() throws Exception {
        AtomicLong clock = new AtomicLong();
        PaimonQueryLimits defaults = new PaimonQueryLimits(Collections.emptyMap(), clock::get);
        PaimonQueryLimits zero = new PaimonQueryLimits(Collections.singletonMap("queryTimeoutSeconds", 0), clock::get);
        clock.set(600_000_000_000L);
        defaults.check();
        zero.check();
        PaimonQueryLimits legacy = defaults.forLegacyQuery();
        assertEquals(Long.MAX_VALUE, legacy.maxScanRows);
        assertEquals(Integer.MAX_VALUE, legacy.maxResultRows);
        assertEquals(Integer.MAX_VALUE, defaults.maxResultRows);
        PaimonQueryLimits explicit = new PaimonQueryLimits(Collections.singletonMap("maxResultRows", 7)).forLegacyQuery();
        assertEquals(7, explicit.maxResultRows);
        // ORDER BY buffers rows, so it keeps a real default cap in both new and legacy modes.
        assertEquals(100_000, defaults.maxBufferedRows);
        assertEquals(100_000, legacy.maxBufferedRows);
        assertEquals(5, new PaimonQueryLimits(Collections.singletonMap("maxBufferedRows", 5)).forLegacyQuery().maxBufferedRows);
        assertThrows(IllegalArgumentException.class,
                () -> new PaimonQueryLimits(Collections.singletonMap("maxBufferedRows", 0)));
        assertThrows(IllegalArgumentException.class,
                () -> new PaimonQueryLimits(Collections.singletonMap("queryTimeoutSeconds", -1)));
    }

    @Test
    void orderedQueriesBoundBufferedRowsWhileStreamingAndAggregatesDoNot() throws Exception {
        PaimonQueryLimits bounded = new PaimonQueryLimits(Collections.singletonMap("maxBufferedRows", 2));
        PaimonQueryPlan.Accumulator ordered = plan("SELECT region FROM orders ORDER BY region").accumulator(bounded);
        ordered.add(GenericRow.of(BinaryString.fromString("B")));
        ordered.add(GenericRow.of(BinaryString.fromString("A")));
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> ordered.add(GenericRow.of(BinaryString.fromString("C"))));
        assertTrue(failure.getMessage().contains("maxBufferedRows"));
        // Unordered rows leave in batches, so they are never held for the whole scan.
        PaimonQueryPlan.Accumulator streamed = plan("SELECT region FROM orders").accumulator(bounded);
        for (String region : Arrays.asList("B", "A", "C")) { streamed.add(GenericRow.of(BinaryString.fromString(region))); }
        assertEquals(3, streamed.drain().size());
        assertEquals(0, streamed.bufferedRows());
        // Grouped results are capped by maxGroups and keep one state per group, not one row per input.
        PaimonQueryPlan.Accumulator grouped = plan("SELECT region, COUNT(*) AS n FROM orders GROUP BY region")
                .accumulator(bounded);
        for (int i = 0; i < 10; i++) { grouped.add(row("A", "1.00")); }
        assertEquals(10L, grouped.finish().get(0).get("n"));
    }

    @Test
    void signedZerosShareOneGroupAndOneSortPosition() throws Exception {
        for (DataType floating : Arrays.asList(DataTypes.FLOAT(), DataTypes.DOUBLE())) {
            boolean single = floating.getTypeRoot().name().equals("FLOAT");
            RowType type = RowType.of(new DataType[]{floating}, new String[]{"v"});
            Object positive = single ? (Object) Float.valueOf(0.0f) : (Object) Double.valueOf(0.0d);
            Object negative = single ? (Object) Float.valueOf(-0.0f) : (Object) Double.valueOf(-0.0d);
            PaimonQueryPlan.Accumulator grouped = PaimonSqlQueryParser.parse(
                    "SELECT v, COUNT(*) AS n, MIN(v) AS lo, MAX(v) AS hi FROM orders GROUP BY v", type)
                    .plan().accumulator(limits());
            grouped.add(GenericRow.of(positive));
            grouped.add(GenericRow.of(negative));
            List<Map<String, Object>> groups = grouped.finish();
            assertEquals(1, groups.size());
            assertEquals(2L, groups.get(0).get("n"));
            // Both zeros are the same value, so MIN/MAX may report either sign.
            assertTrue(((Number) groups.get(0).get("lo")).doubleValue() == 0.0d);
            assertTrue(((Number) groups.get(0).get("hi")).doubleValue() == 0.0d);
            assertEquals(groups.get(0).get("lo").getClass(), groups.get(0).get("hi").getClass());
            PaimonQueryPlan.Accumulator sorted = PaimonSqlQueryParser.parse("SELECT v FROM orders ORDER BY v", type)
                    .plan().accumulator(limits());
            sorted.add(GenericRow.of(positive));
            sorted.add(GenericRow.of(negative));
            List<Map<String, Object>> rows = sorted.finish();
            assertEquals(2, rows.size());
            // Equal values keep scan order, so the +0.0 row that was read first stays first.
            assertEquals(Double.doubleToRawLongBits(0.0d),
                    Double.doubleToRawLongBits(((Number) rows.get(0).get("v")).doubleValue()));
            assertEquals(Double.doubleToRawLongBits(-0.0d),
                    Double.doubleToRawLongBits(((Number) rows.get(1).get("v")).doubleValue()));
        }
    }

    @Test
    void deadlineIsCheckedInsideSortAndCheckedExceptionIsPreserved() {
        AtomicLong ticks = new AtomicLong();
        PaimonQueryLimits limits = new PaimonQueryLimits(Collections.singletonMap("queryTimeoutSeconds", 1),
                () -> ticks.getAndAdd(400_000_000L));
        PaimonQueryPlan.Accumulator accumulator = plan("SELECT region FROM orders ORDER BY region").accumulator(limits);
        accumulator.add(GenericRow.of(BinaryString.fromString("B")));
        accumulator.add(GenericRow.of(BinaryString.fromString("A")));
        SQLTimeoutException failure = assertThrows(SQLTimeoutException.class, accumulator::finish);
        assertTrue(failure.getMessage().contains("queryTimeoutSeconds=1"));
        // Constructor, finish, pre-sort and first comparator: expiry occurs during comparison.
        assertEquals(1_600_000_000L, ticks.get());
    }

    @Test
    void binaryMinMaxUsesUnsignedLexicographicOrdering() throws Exception {
        RowType type = RowType.of(new DataType[]{DataTypes.BYTES()}, new String[]{"payload"});
        PaimonQueryPlan.Accumulator accumulator = PaimonSqlQueryParser.parse(
                "SELECT MIN(payload) AS lo, MAX(payload) AS hi FROM orders", type).plan().accumulator(limits());
        accumulator.add(GenericRow.of(new byte[]{(byte) 255}));
        accumulator.add(GenericRow.of(new byte[]{0, 1}));
        accumulator.add(GenericRow.of(new byte[]{0}));
        Map<String, Object> result = accumulator.finish().get(0);
        assertArrayEquals(new byte[]{0}, (byte[]) result.get("lo"));
        assertArrayEquals(new byte[]{(byte) 255}, (byte[]) result.get("hi"));
    }

    @Test
    void preservesScalarTemporalTypesWhileDateCastReturnsLocalDate() throws Exception {
        RowType type = RowType.of(new DataType[]{DataTypes.DATE(), DataTypes.TIME(3),
                DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)},
                new String[]{"d", "t", "ts", "ltz"});
        java.time.LocalDate date = java.time.LocalDate.of(2026, 4, 2);
        java.time.LocalDateTime timestamp = date.atTime(12, 34, 56);
        GenericRow row = GenericRow.of((int) date.toEpochDay(), 45_296_000,
                org.apache.paimon.data.Timestamp.fromLocalDateTime(timestamp),
                org.apache.paimon.data.Timestamp.fromInstant(timestamp.toInstant(java.time.ZoneOffset.UTC)));
        for (String sql : Arrays.asList("SELECT * FROM orders", "SELECT d, t, ts, ltz FROM orders ORDER BY ltz")) {
            PaimonQueryPlan.Accumulator accumulator = PaimonSqlQueryParser.parse(sql, type).plan().accumulator(limits());
            accumulator.add(row);
            Map<String, Object> result = accumulator.finish().get(0);
            assertEquals(java.sql.Date.class, result.get("d").getClass());
            assertEquals(date.toEpochDay() * 86400000L, ((java.sql.Date) result.get("d")).getTime());
            assertEquals(java.time.LocalDate.of(1970, 1, 1).atTime(12, 34, 56), result.get("t"));
            assertEquals(timestamp, result.get("ts"));
            assertEquals(timestamp.atZone(java.time.ZoneOffset.UTC), result.get("ltz"));
        }
        PaimonQueryPlan.Accumulator cast = PaimonSqlQueryParser.parse("SELECT CAST(d AS DATE) AS day FROM orders", type)
                .plan().accumulator(limits());
        cast.add(GenericRow.of((int) date.toEpochDay()));
        assertEquals(date, cast.finish().get(0).get("day"));
    }

    @Test
    void floatingAggregatesAndSortHandleFiniteAndNonFiniteValuesExplicitly() throws Exception {
        for (DataType floating : Arrays.asList(DataTypes.FLOAT(), DataTypes.DOUBLE())) {
            RowType type = RowType.of(new DataType[]{floating}, new String[]{"v"});
            boolean single = floating.getTypeRoot().name().equals("FLOAT");
            for (String function : Arrays.asList("SUM", "AVG")) {
                for (double special : new double[]{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
                    PaimonQueryPlan.Accumulator a = PaimonSqlQueryParser.parse("SELECT " + function + "(v) AS n FROM orders", type)
                            .plan().accumulator(limits());
                    Object input = single ? (Object) (float) special : special;
                    IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> a.add(GenericRow.of(input)));
                    assertTrue(error.getMessage().contains("requires finite numeric values"));
                }
                PaimonQueryPlan.Accumulator a = PaimonSqlQueryParser.parse("SELECT " + function + "(v) AS n FROM orders", type)
                        .plan().accumulator(limits());
                for (double n : new double[]{-0.0, 1.5, 3.0}) { a.add(GenericRow.of(single ? (Object) (float) n : n)); }
                BigDecimal expected = new BigDecimal(function.equals("SUM") ? "4.5" : "1.5");
                assertEquals(0, expected.compareTo((BigDecimal) a.finish().get(0).get("n")));
            }
            PaimonQueryPlan.Accumulator a = PaimonSqlQueryParser.parse("SELECT MIN(v) AS lo, MAX(v) AS hi FROM orders", type)
                    .plan().accumulator(limits());
            PaimonQueryPlan.Accumulator sorted = PaimonSqlQueryParser.parse("SELECT v FROM orders ORDER BY v", type)
                    .plan().accumulator(limits());
            for (double n : new double[]{Double.NaN, 0.0, Double.POSITIVE_INFINITY, -0.0, Double.NEGATIVE_INFINITY, 2.0}) {
                GenericRow input = GenericRow.of(single ? (Object) (float) n : n);
                a.add(input); sorted.add(input);
            }
            Map<String, Object> result = a.finish().get(0);
            assertEquals(Double.NEGATIVE_INFINITY, ((Number) result.get("lo")).doubleValue());
            assertTrue(Double.isNaN(((Number) result.get("hi")).doubleValue()));
            List<Map<String, Object>> rows = sorted.finish();
            assertEquals(Double.NEGATIVE_INFINITY, ((Number) rows.get(0).get("v")).doubleValue());
            assertEquals(Double.POSITIVE_INFINITY, ((Number) rows.get(4).get("v")).doubleValue());
            assertTrue(Double.isNaN(((Number) rows.get(5).get("v")).doubleValue()));
        }
    }

    @Test
    void resolvesUnquotedAliasesIgnoringCaseAndQuotedAliasesExactly() throws Exception {
        for (String alias : Arrays.asList("amount", "AMOUNT", "\"Amount\"")) {
            PaimonQueryPlan.Accumulator a = plan("SELECT SUM(amount) AS Amount FROM orders ORDER BY " + alias).accumulator(limits());
            a.add(GenericRow.of(Decimal.fromBigDecimal(new BigDecimal("1.00"), 20, 2)));
            assertEquals(new BigDecimal("1.00"), a.finish().get(0).get("Amount"));
        }
        assertThrows(IllegalArgumentException.class, () -> plan("SELECT SUM(amount) AS Amount FROM orders ORDER BY \"amount\""));
    }

    @Test
    void reversedKeyComparisonsArePushedDownWithCorrectDirection() {
        for (String operator : Arrays.asList("=", "<", "<=", ">", ">=")) {
            String reverse = operator.equals("<") ? ">" : operator.equals("<=") ? ">="
                    : operator.equals(">") ? "<" : operator.equals(">=") ? "<=" : "=";
            PaimonSqlQueryParser.ParsedQuery query = PaimonSqlQueryParser.parse(
                    "SELECT SUM(amount) FROM orders WHERE 5 " + operator + " unused", TYPE);
            assertTrue(query.plan().hasPruningFilter(Collections.singletonList("unused"), Collections.emptyList()));
            assertEquals(PaimonSqlQueryParser.parse("SELECT SUM(amount) FROM orders WHERE unused " + reverse + " 5", TYPE).predicate(),
                    query.predicate());
        }
    }

    @Test
    void escapedIdentifierQuotesAreDecodedOnce() throws Exception {
        RowType type = RowType.of(new DataType[]{DataTypes.INT()}, new String[]{"col\"name"});
        // JSqlParser 4.4 rejects doubled double quotes before planning; keep this boundary explicit.
        assertThrows(IllegalArgumentException.class, () -> PaimonSqlQueryParser.parse(
                "SELECT \"col\"\"name\" FROM orders", type));
        assertEquals("col\"name", PaimonSqlQueryParser.unquote("\"col\"\"name\""));
        assertEquals("a]b", PaimonSqlQueryParser.unquote("[a]]b]"));
        assertEquals("a`b", PaimonSqlQueryParser.unquote("`a``b`"));
    }

    private static PaimonQueryPlan plan(String sql) { return PaimonSqlQueryParser.parse(sql, TYPE).plan(); }
    private static PaimonQueryLimits limits() { return new PaimonQueryLimits(Collections.emptyMap()); }
    private static GenericRow row(String region, String amount) {
        return GenericRow.of(region == null ? null : BinaryString.fromString(region), amount == null ? null
                : Decimal.fromBigDecimal(new BigDecimal(amount), 20, 2));
    }
}
