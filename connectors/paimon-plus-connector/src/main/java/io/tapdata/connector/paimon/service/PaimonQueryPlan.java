package io.tapdata.connector.paimon.service;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.math.MathContext;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Bounded single-table SQL plan. Only group state (never input rows) is retained for aggregates. */
final class PaimonQueryPlan {
    private final RowType rowType;
    private final String table;
    private final List<Item> items = new ArrayList<>();
    private final List<Scalar> groups = new ArrayList<>();
    private final List<Sort> sorts = new ArrayList<>();
    private final Set<Integer> columns = new LinkedHashSet<>();
    private Expression where;
    private boolean aggregate;
    private boolean star;

    private PaimonQueryPlan(RowType rowType, String table) {
        this.rowType = rowType;
        this.table = table;
    }

    static PaimonQueryPlan parse(PlainSelect select, RowType rowType, String table) {
        PaimonSqlQueryParser.validatePlainSelect(select, true);
        PaimonQueryPlan plan = new PaimonQueryPlan(rowType, table);
        plan.where = select.getWhere();
        if (select.getGroupBy() != null) {
            if (select.getGroupBy().isUsingBrackets()
                    || select.getGroupBy().getGroupingSets() != null && !select.getGroupBy().getGroupingSets().isEmpty()) {
                throw unsupported("GROUPING SETS are not supported");
            }
            for (Expression expression : select.getGroupBy().getGroupByExpressionList().getExpressions()) {
                plan.groups.add(plan.scalar(expression));
            }
            plan.aggregate = true;
        }
        Set<String> labels = new HashSet<>();
        for (SelectItem item : select.getSelectItems()) {
            if (item instanceof AllColumns && select.getSelectItems().size() == 1) {
                plan.star = true;
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    plan.items.add(new Item(rowType.getFieldNames().get(i), plan.column(i), null, rowType.getFieldNames().get(i)));
                }
            } else if (item instanceof SelectExpressionItem) {
                SelectExpressionItem expressionItem = (SelectExpressionItem) item;
                Expression expression = expressionItem.getExpression();
                String label = expressionItem.getAlias() == null ? expression.toString()
                        : PaimonSqlQueryParser.unquote(expressionItem.getAlias().getName());
                if (!labels.add(label)) {
                    throw unsupported("duplicate result label; assign unique aliases: " + label);
                }
                if (expression instanceof Function) {
                    Function function = (Function) expression;
                    String name = function.getName().toUpperCase(Locale.ROOT);
                    if (!Arrays.asList("COUNT", "SUM", "MIN", "MAX", "AVG").contains(name)
                            || function.isDistinct() || function.isUnique() || function.isIgnoreNulls()
                            || function.getAttribute() != null || function.getKeep() != null || function.getNamedParameters() != null
                            || function.getOrderByElements() != null) {
                        throw unsupported("unsupported aggregate: " + expression);
                    }
                    List<Expression> parameters = function.getParameters() == null ? Collections.emptyList()
                            : function.getParameters().getExpressions();
                    boolean all = parameters.size() == 1 && parameters.get(0) instanceof AllColumns;
                    boolean one = parameters.size() == 1 && parameters.get(0) instanceof LongValue
                            && ((LongValue) parameters.get(0)).getValue() == 1;
                    Scalar argument = null;
                    if (!"COUNT".equals(name) || !all && !one) {
                        if (all || parameters.size() != 1) {
                            throw unsupported(name + " requires a single column");
                        }
                        argument = plan.scalar(parameters.get(0));
                        if (("SUM".equals(name) || "AVG".equals(name)) && !argument.numeric()) {
                            throw unsupported(name + " requires a numeric column");
                        }
                    }
                    plan.items.add(new Item(label, argument, name, expression.toString()));
                    plan.aggregate = true;
                } else {
                    plan.items.add(new Item(label, plan.scalar(expression), null, expression.toString()));
                }
            } else {
                throw unsupported("only column expressions and aggregates are supported");
            }
        }
        if (plan.aggregate) {
            if (plan.star) { throw unsupported("SELECT * cannot be grouped"); }
            for (Item item : plan.items) {
                if (item.function == null && plan.groups.stream().noneMatch(g -> g.key().equals(item.scalar.key()))) {
                    throw unsupported("non-aggregate expression must appear in GROUP BY: " + item.label);
                }
            }
        }
        if (select.getOrderByElements() != null) {
            for (OrderByElement order : select.getOrderByElements()) {
                int index = plan.orderIndex(order.getExpression());
                boolean nullsFirst = order.getNullOrdering() == OrderByElement.NullOrdering.NULLS_FIRST
                        || order.getNullOrdering() == null && order.isAsc();
                plan.sorts.add(new Sort(index, order.isAsc(), nullsFirst));
            }
        }
        // Keep at least one physical column for COUNT(*) on implementations without zero-column scans.
        if (plan.columns.isEmpty()) { plan.columns.add(0); }
        return plan;
    }

    private int orderIndex(Expression expression) {
        if (expression instanceof LongValue) {
            long ordinal = ((LongValue) expression).getValue();
            if (ordinal >= 1 && ordinal <= items.size()) { return (int) ordinal - 1; }
            throw unsupported("ORDER BY ordinal is out of range");
        }
        if (expression instanceof Column) {
            Column column = (Column) expression;
            // ORDER BY must not silently resolve another table's column to a same-named projection.
            Table qualifier = column.getTable();
            if (qualifier != null && qualifier.getName() != null
                    && (!PaimonSqlQueryParser.unquote(qualifier.getName()).equals(table)
                    || qualifier.getSchemaName() != null)) {
                throw unsupported("ORDER BY references another table: " + column);
            }
            for (int i = 0; i < items.size(); i++) {
                if (aliasMatches(items.get(i).label, column.getColumnName())) { return i; }
            }
        } else {
            for (int i = 0; i < items.size(); i++) {
                if (items.get(i).expressionSql.equalsIgnoreCase(expression.toString())) { return i; }
            }
        }
        Scalar scalar = scalar(expression);
        for (int i = 0; i < items.size(); i++) {
            Item item = items.get(i);
            if (item.function == null && item.scalar.key().equals(scalar.key())) { return i; }
        }
        throw unsupported("ORDER BY must reference a selected expression or alias");
    }

    private static boolean aliasMatches(String label, String identifier) {
        String unquoted = PaimonSqlQueryParser.unquote(identifier);
        return identifier.equals(unquoted) ? label.equalsIgnoreCase(unquoted) : label.equals(unquoted);
    }

    private Scalar column(int index) {
        columns.add(index);
        DataType type = rowType.getTypeAt(index);
        String root = type.getTypeRoot().name();
        if (Arrays.asList("ARRAY", "MAP", "MULTISET", "ROW").contains(root)) {
            throw unsupported("complex column types are not supported; explicitly select scalar columns instead: " + rowType.getFieldNames().get(index));
        }
        return new Scalar(index, type, false);
    }

    private Scalar scalar(Expression expression) {
        if (expression instanceof Column) {
            return column(PaimonSqlQueryParser.fieldIndex(expression, table, rowType, new PredicateBuilder(rowType)));
        }
        if (expression instanceof CastExpression) {
            CastExpression cast = (CastExpression) expression;
            if (cast.getType() == null || !"DATE".equalsIgnoreCase(cast.getType().getDataType())
                    || !(cast.getLeftExpression() instanceof Column)) {
                throw unsupported("only CAST(column AS DATE) is supported");
            }
            Scalar source = scalar(cast.getLeftExpression());
            String root = source.type.getTypeRoot().name();
            if (!"DATE".equals(root) && !"TIMESTAMP_WITHOUT_TIME_ZONE".equals(root)) {
                throw unsupported("CAST AS DATE requires DATE or TIMESTAMP without time zone");
            }
            return new Scalar(source.index, source.type, true);
        }
        throw unsupported("unsupported expression: " + expression);
    }

    boolean hasPruningFilter(List<String> partitionKeys, List<String> primaryKeys) {
        Set<String> keys = new HashSet<>(partitionKeys);
        // This is eligibility, not proof of pruning. Only the leading PK is considered.
        if (!primaryKeys.isEmpty()) { keys.add(primaryKeys.get(0)); }
        return pruningFilter(where, keys);
    }

    private boolean pruningFilter(Expression expression, Set<String> keys) {
        if (expression instanceof net.sf.jsqlparser.expression.Parenthesis) {
            return pruningFilter(((net.sf.jsqlparser.expression.Parenthesis) expression).getExpression(), keys);
        }
        if (expression instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression) {
            net.sf.jsqlparser.expression.BinaryExpression and = (net.sf.jsqlparser.expression.BinaryExpression) expression;
            return pruningFilter(and.getLeftExpression(), keys) || pruningFilter(and.getRightExpression(), keys);
        }
        if (expression instanceof net.sf.jsqlparser.expression.operators.conditional.OrExpression) {
            net.sf.jsqlparser.expression.BinaryExpression or = (net.sf.jsqlparser.expression.BinaryExpression) expression;
            // Every OR branch must constrain an eligible key; one unconstrained branch
            // could still read the whole table.
            return pruningFilter(or.getLeftExpression(), keys) && pruningFilter(or.getRightExpression(), keys);
        }
        Expression column = null;
        if (expression instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo
                || expression instanceof net.sf.jsqlparser.expression.operators.relational.GreaterThan
                || expression instanceof net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals
                || expression instanceof net.sf.jsqlparser.expression.operators.relational.MinorThan
                || expression instanceof net.sf.jsqlparser.expression.operators.relational.MinorThanEquals) {
            net.sf.jsqlparser.expression.BinaryExpression comparison = (net.sf.jsqlparser.expression.BinaryExpression) expression;
            column = comparison.getLeftExpression() instanceof Column
                    ? comparison.getLeftExpression() : comparison.getRightExpression();
        } else if (expression instanceof net.sf.jsqlparser.expression.operators.relational.Between) {
            net.sf.jsqlparser.expression.operators.relational.Between between = (net.sf.jsqlparser.expression.operators.relational.Between) expression;
            if (!between.isNot()) { column = between.getLeftExpression(); }
        } else if (expression instanceof net.sf.jsqlparser.expression.operators.relational.InExpression) {
            net.sf.jsqlparser.expression.operators.relational.InExpression in = (net.sf.jsqlparser.expression.operators.relational.InExpression) expression;
            if (!in.isNot()) { column = in.getLeftExpression(); }
        } else if (expression instanceof net.sf.jsqlparser.expression.operators.relational.IsNullExpression) {
            net.sf.jsqlparser.expression.operators.relational.IsNullExpression isNull =
                    (net.sf.jsqlparser.expression.operators.relational.IsNullExpression) expression;
            if (!isNull.isNot()) { column = isNull.getLeftExpression(); }
        }
        return column instanceof Column && keys.contains(PaimonSqlQueryParser.unquote(((Column) column).getColumnName()));
    }

    int[] projection() { return columns.stream().mapToInt(Integer::intValue).toArray(); }
    boolean aggregate() { return aggregate; }
    boolean streamable() { return !aggregate && sorts.isEmpty(); }
    boolean legacyRows() { return star && sorts.isEmpty() && !aggregate; }
    Accumulator accumulator(PaimonQueryLimits limits) { return new Accumulator(limits, projection()); }
    Accumulator accumulator(PaimonQueryLimits limits, int[] projection) { return new Accumulator(limits, projection); }

    static void addPredicateColumns(org.apache.paimon.predicate.Predicate predicate, Set<Integer> columns) {
        if (predicate instanceof org.apache.paimon.predicate.LeafPredicate) {
            columns.add(((org.apache.paimon.predicate.LeafPredicate) predicate).index());
        } else if (predicate instanceof org.apache.paimon.predicate.CompoundPredicate) {
            for (org.apache.paimon.predicate.Predicate child : ((org.apache.paimon.predicate.CompoundPredicate) predicate).children()) {
                addPredicateColumns(child, columns);
            }
        }
    }

    final class Accumulator {
        private final PaimonQueryLimits limits;
        private final Map<List<Object>, State[]> states = new LinkedHashMap<>();
        private final List<Object[]> rows = new ArrayList<>();
        private long resultRows;
        private final int[] projected;
        private final InternalRow.FieldGetter[] getters;
        private final int[] positions = new int[rowType.getFieldCount()];

        private Accumulator(PaimonQueryLimits limits, int[] projected) {
            this.limits = limits;
            this.projected = projected;
            getters = new InternalRow.FieldGetter[projected.length];
            for (int i = 0; i < projected.length; i++) {
                positions[projected[i]] = i;
                getters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(projected[i]), i);
            }
            if (aggregate && groups.isEmpty()) { states.put(Collections.emptyList(), newStates()); }
        }

        void add(InternalRow row) {
            Object[] values = new Object[projected.length];
            for (int i = 0; i < values.length; i++) { values[i] = javaValue(getters[i].getFieldOrNull(row), rowType.getTypeAt(projected[i])); }
            if (!aggregate) {
                if (resultRows++ >= limits.maxResultRows) { throw limits.exceeded("maxResultRows", limits.maxResultRows); }
                if (!streamable() && rows.size() >= limits.maxBufferedRows) {
                    throw limits.exceeded("maxBufferedRows", limits.maxBufferedRows);
                }
                Object[] result = new Object[items.size()];
                for (int i = 0; i < result.length; i++) { result[i] = value(items.get(i).scalar, values); }
                rows.add(result);
                return;
            }
            List<Object> key = new ArrayList<>(groups.size());
            for (Scalar group : groups) { key.add(groupKey(value(group, values))); }
            State[] state = states.get(key);
            if (state == null) {
                if (states.size() >= limits.maxGroups) { throw limits.exceeded("maxGroups", limits.maxGroups); }
                state = newStates();
                states.put(key, state);
            }
            for (int i = 0; i < items.size(); i++) {
                Item item = items.get(i);
                state[i].add(item.scalar == null ? null : value(item.scalar, values));
            }
        }

        /**
         * Grouping equality must follow SQL semantics: -0.0 and +0.0 are the same value, so keys
         * normalize signed zeros; every NaN still shares one group and binary keys compare by bytes.
         */
        private Object groupKey(Object part) {
            return part == null ? null : part instanceof byte[] ? java.nio.ByteBuffer.wrap((byte[]) part) : normalizeZero(part);
        }

        private Object value(Scalar scalar, Object[] values) {
            Object value = values[positions[scalar.index]];
            if (scalar.date && value != null && value instanceof java.time.LocalDateTime) {
                return ((java.time.LocalDateTime) value).toLocalDate();
            }
            return value;
        }

        private State[] newStates() {
            State[] result = new State[items.size()];
            for (int i = 0; i < result.length; i++) { result[i] = new State(items.get(i)); }
            return result;
        }

        int groupCount() { return states.size(); }

        int bufferedRows() { return rows.size(); }

        List<Map<String, Object>> drain() throws java.sql.SQLTimeoutException, InterruptedException {
            if (!streamable()) throw new IllegalStateException("Only unordered projection rows can be drained");
            List<Map<String, Object>> batch = finish();
            rows.clear();
            return batch;
        }

        List<Map<String, Object>> finish() throws java.sql.SQLTimeoutException, InterruptedException {
            limits.check();
            if (aggregate) {
                if (states.size() > limits.maxResultRows) { throw limits.exceeded("maxResultRows", limits.maxResultRows); }
                // ORDER BY materializes one row per group before sorting, so the ordered buffer cap
                // applies to aggregate results as well as to plain projections.
                if (!sorts.isEmpty() && states.size() > limits.maxBufferedRows) {
                    throw limits.exceeded("maxBufferedRows", limits.maxBufferedRows);
                }
                for (State[] state : states.values()) {
                    limits.check();
                    Object[] result = new Object[items.size()];
                    for (int i = 0; i < result.length; i++) { result[i] = state[i].result(); }
                    rows.add(result);
                }
            }
            if (!sorts.isEmpty()) {
                limits.check();
                // Polling the deadline (and the interruption flag) on every comparison would add a
                // clock read per comparison; sampling keeps cancellation responsive at O(n log n / 1024).
                int[] comparisons = {0};
                Comparator<Object[]> comparator = (a, b) -> {
                    if ((comparisons[0]++ & 1023) == 0) {
                        try { limits.check(); }
                        catch (java.sql.SQLTimeoutException | InterruptedException failure) {
                            throw new SortCancelled(failure);
                        }
                    }
                    for (Sort sort : sorts) {
                        Object left = a[sort.index], right = b[sort.index];
                        int order;
                        if (left == null || right == null) {
                            order = left == right ? 0 : left == null ? (sort.nullsFirst ? -1 : 1) : (sort.nullsFirst ? 1 : -1);
                        } else {
                            order = compare(left, right) * (sort.ascending ? 1 : -1);
                        }
                        if (order != 0) { return order; }
                    }
                    return 0;
                };
                // Cancellation propagates out of List.sort; the partially sorted private buffer
                // is discarded on failure and is never exposed to the caller.
                try { rows.sort(comparator); }
                catch (SortCancelled failure) {
                    if (failure.getCause() instanceof InterruptedException) {
                        throw (InterruptedException) failure.getCause();
                    }
                    throw (java.sql.SQLTimeoutException) failure.getCause();
                }
                limits.check();
            }
            List<Map<String, Object>> result = new ArrayList<>(rows.size());
            for (Object[] row : rows) {
                limits.check();
                Map<String, Object> mapped = new LinkedHashMap<>();
                for (int i = 0; i < row.length; i++) { mapped.put(items.get(i).label, resultValue(items.get(i), row[i])); }
                result.add(mapped);
            }
            return result;
        }
    }

    // Preserve the connector's existing scalar return types for both star and projected queries.
    // CAST AS DATE is an expression and deliberately returns LocalDate, independent of JVM zone.
    private static Object resultValue(Item item, Object value) {
        if (value == null || item.scalar == null || item.scalar.date
                || "SUM".equals(item.function) || "AVG".equals(item.function)
                || "COUNT".equals(item.function)) { return value; }
        switch (item.scalar.type.getTypeRoot().name()) {
            case "DATE": return new java.sql.Date(((LocalDate) value).toEpochDay() * 86400000L);
            case "TIME_WITHOUT_TIME_ZONE":
                return ((java.time.LocalTime) value).atDate(LocalDate.of(1970, 1, 1));
            case "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
                return ((java.time.Instant) value).atZone(java.time.ZoneOffset.UTC);
            default: return value;
        }
    }

    private static final class SortCancelled extends RuntimeException {
        private SortCancelled(Exception cause) { super(cause); }
    }

    private static Object javaValue(Object value, DataType type) {
        if (value instanceof BinaryString) { return value.toString(); }
        if (value instanceof Decimal) { return ((Decimal) value).toBigDecimal(); }
        if (value instanceof Timestamp) {
            return "TIMESTAMP_WITH_LOCAL_TIME_ZONE".equals(type.getTypeRoot().name())
                    ? ((Timestamp) value).toInstant() : ((Timestamp) value).toLocalDateTime();
        }
        if (value != null && "TIME_WITHOUT_TIME_ZONE".equals(type.getTypeRoot().name())) {
            return java.time.LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L);
        }
        if (value != null && "DATE".equals(type.getTypeRoot().name())) { return LocalDate.ofEpochDay(((Number) value).longValue()); }
        return value;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            if (fractional(left) && fractional(right)) {
                return compareFloating(((Number) left).doubleValue(), ((Number) right).doubleValue());
            }
            // Mixed floating/exact comparison stays exact; decimal() rejects non-finite values explicitly.
            return decimal(left).compareTo(decimal(right));
        }
        if (left instanceof byte[] && right instanceof byte[]) {
            byte[] a = (byte[]) left, b = (byte[]) right;
            for (int i = 0; i < Math.min(a.length, b.length); i++) {
                int order = Integer.compare(a[i] & 0xff, b[i] & 0xff);
                if (order != 0) { return order; }
            }
            return Integer.compare(a.length, b.length);
        }
        if (left instanceof byte[] || right instanceof byte[]) {
            throw new IllegalArgumentException("incomparable types: " + left.getClass().getName()
                    + " and " + right.getClass().getName());
        }
        return ((Comparable) left).compareTo(right);
    }

    /** Total order over floating values: -Infinity < finite values < +Infinity < NaN; signed zeros compare equal. */
    private static int compareFloating(double left, double right) {
        // Double.compare alone distinguishes -0.0 from 0.0, which would split groups and reorder equal rows.
        return left == right ? 0 : Double.compare(left, right);
    }

    private static boolean fractional(Object value) {
        return value instanceof Double || value instanceof Float;
    }

    private static boolean nonFinite(Object value) {
        return (value instanceof Double || value instanceof Float)
                && !Double.isFinite(((Number) value).doubleValue());
    }

    private static BigDecimal decimal(Object value) {
        if (nonFinite(value)) {
            throw new IllegalArgumentException("Paimon SUM/AVG requires finite numeric values; encountered " + value);
        }
        return value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString());
    }

    /** Signed zeros are one SQL value; normalize them so grouping, ordering and display agree. */
    private static Object normalizeZero(Object value) {
        if (value instanceof Double) {
            return (Double) value == 0.0d ? Double.valueOf(0.0d) : value;
        }
        if (value instanceof Float) {
            return (Float) value == 0.0f ? Float.valueOf(0.0f) : value;
        }
        return value;
    }

    private static final class State {
        private final Item item;
        private long count;
        private Object value;
        private State(Item item) { this.item = item; }
        void add(Object next) {
            // A grouped column shows one value per group; normalize signed zeros so the chosen
            // sign never depends on scan order.
            if (item.function == null) { value = normalizeZero(next); return; }
            if ("COUNT".equals(item.function)) {
                if (item.scalar == null || next != null) { count = Math.addExact(count, 1); }
                return;
            }
            if (next == null) { return; }
            count = Math.addExact(count, 1);
            switch (item.function) {
                case "SUM": case "AVG":
                    BigDecimal number = decimal(next);
                    value = value == null ? number : ((BigDecimal) value).add(number);
                    break;
                case "MIN": if (value == null || compare(next, value) < 0) { value = next; } break;
                case "MAX": if (value == null || compare(next, value) > 0) { value = next; } break;
                default: throw new IllegalStateException(item.function);
            }
        }
        Object result() {
            if ("COUNT".equals(item.function)) { return count; }
            if ("AVG".equals(item.function) && value != null) {
                return ((BigDecimal) value).divide(BigDecimal.valueOf(count), MathContext.DECIMAL128);
            }
            return value;
        }
    }

    private static final class Item {
        final String label;
        final Scalar scalar;
        final String function;
        final String expressionSql;
        Item(String label, Scalar scalar, String function, String expressionSql) {
            this.label = label;
            this.scalar = scalar;
            this.function = function;
            this.expressionSql = expressionSql;
        }
    }
    private static final class Scalar {
        final int index;
        final DataType type;
        final boolean date;
        Scalar(int index, DataType type, boolean date) { this.index = index; this.type = type; this.date = date; }
        String key() { return index + ":" + date; }
        boolean numeric() { return Arrays.asList("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL").contains(type.getTypeRoot().name()) && !date; }
    }
    private static final class Sort {
        final int index;
        final boolean ascending;
        final boolean nullsFirst;
        Sort(int index, boolean ascending, boolean nullsFirst) { this.index = index; this.ascending = ascending; this.nullsFirst = nullsFirst; }
    }
    private static IllegalArgumentException unsupported(String message) {
        return new IllegalArgumentException("Unsupported Paimon SQL: " + message);
    }
}
