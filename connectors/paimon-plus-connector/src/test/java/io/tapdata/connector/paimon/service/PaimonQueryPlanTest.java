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
        assertThrows(IllegalArgumentException.class,
                () -> new PaimonQueryLimits(Collections.singletonMap("allowFullScan", "true")));
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
    void detectsEligibleKeyFiltersWithoutTreatingOrAsPruning() {
        assertTrue(plan("SELECT SUM(amount) FROM orders WHERE region = 'A'")
                .hasPruningFilter(Collections.singletonList("region"), Collections.emptyList()));
        assertFalse(plan("SELECT SUM(amount) FROM orders WHERE region = 'A' OR amount > 1")
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

    private static PaimonQueryPlan plan(String sql) { return PaimonSqlQueryParser.parse(sql, TYPE).plan(); }
    private static PaimonQueryLimits limits() { return new PaimonQueryLimits(Collections.emptyMap()); }
    private static GenericRow row(String region, String amount) {
        return GenericRow.of(region == null ? null : BinaryString.fromString(region), amount == null ? null
                : Decimal.fromBigDecimal(new BigDecimal(amount), 20, 2));
    }
}
