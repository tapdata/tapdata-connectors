package io.tapdata.connector.paimon.service;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses the deliberately small read-only SQL dialect exposed by the Paimon connector.
 *
 * <p>The parser rejects unsupported constructs instead of returning a query that would scan the
 * whole table. This is important because the result is used by the data validation count path.
 */
final class PaimonSqlQueryParser {

    private PaimonSqlQueryParser() {
    }

    static ParsedQuery parse(String sql, RowType rowType) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("Paimon SQL must not be empty");
        }
        if (rowType == null) {
            throw new IllegalArgumentException("Paimon row type must not be null");
        }

        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (!(statement instanceof Select)) {
                throw unsupported("only SELECT statements are supported");
            }

            Select select = (Select) statement;
            if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
                throw unsupported("WITH queries are not supported");
            }

            SelectBody body = select.getSelectBody();
            if (!(body instanceof PlainSelect)) {
                throw unsupported("only a single plain SELECT is supported");
            }

            PlainSelect outer = (PlainSelect) body;
            if (isCountWrapper(outer)) {
                return parseCountWrapper(outer, rowType);
            }
            return parseRows(outer, rowType);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw unsupported("invalid SELECT syntax", e);
        }
    }

    static String tableName(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("Paimon SQL must not be empty");
        }
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (!(statement instanceof Select)) {
                throw unsupported("only SELECT statements are supported");
            }
            Select select = (Select) statement;
            if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
                throw unsupported("WITH queries are not supported");
            }
            if (!(select.getSelectBody() instanceof PlainSelect)) {
                throw unsupported("only a single plain SELECT is supported");
            }
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
            if (isCountWrapper(plainSelect)) {
                SubSelect subSelect = (SubSelect) plainSelect.getFromItem();
                if (!(subSelect.getSelectBody() instanceof PlainSelect)) {
                    throw unsupported("COUNT must wrap a plain SELECT");
                }
                plainSelect = (PlainSelect) subSelect.getSelectBody();
            }
            return requireTable(plainSelect.getFromItem()).getName();
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw unsupported("invalid SELECT syntax", e);
        }
    }

    private static ParsedQuery parseRows(PlainSelect select, RowType rowType) {
        validatePlainSelect(select);
        List<SelectItem> items = select.getSelectItems();
        if (items == null || items.size() != 1 || !(items.get(0) instanceof AllColumns)) {
            throw unsupported("only SELECT * is supported");
        }

        Table table = requireTable(select.getFromItem());
        Expression where = select.getWhere();
        if (where == null) {
            throw unsupported("a WHERE predicate is required");
        }

        PredicateBuilder builder = new PredicateBuilder(rowType);
        return new ParsedQuery(table.getName(), false,
                parsePredicate(where, table.getName(), rowType, builder));
    }

    private static ParsedQuery parseCountWrapper(PlainSelect outer, RowType rowType) {
        validatePlainSelect(outer);
        if (outer.getWhere() != null) {
            throw unsupported("the count wrapper must not have an outer WHERE");
        }
        if (!(outer.getFromItem() instanceof SubSelect)) {
            throw unsupported("COUNT must wrap a single SELECT");
        }

        SubSelect subSelect = (SubSelect) outer.getFromItem();
        SelectBody body = subSelect.getSelectBody();
        if (!(body instanceof PlainSelect)) {
            throw unsupported("COUNT must wrap a plain SELECT");
        }

        PlainSelect inner = (PlainSelect) body;
        ParsedQuery query = parseRows(inner, rowType);
        return new ParsedQuery(query.tableName(), true, query.predicate());
    }

    private static boolean isCountWrapper(PlainSelect select) {
        if (!(select.getFromItem() instanceof SubSelect)) {
            return false;
        }
        List<SelectItem> items = select.getSelectItems();
        if (items == null || items.size() != 1 || !(items.get(0) instanceof SelectExpressionItem)) {
            return false;
        }
        Expression expression = ((SelectExpressionItem) items.get(0)).getExpression();
        if (!(expression instanceof Function)) {
            return false;
        }
        Function function = (Function) expression;
        if (!"COUNT".equalsIgnoreCase(function.getName()) || function.isDistinct()
                || function.isAllColumns() || function.getParameters() == null) {
            return false;
        }
        List<Expression> parameters = function.getParameters().getExpressions();
        return parameters != null && parameters.size() == 1 && parameters.get(0) instanceof LongValue
                && ((LongValue) parameters.get(0)).getValue() == 1L;
    }

    private static void validatePlainSelect(PlainSelect select) {
        if (select.getJoins() != null && !select.getJoins().isEmpty()) {
            throw unsupported("JOIN is not supported");
        }
        if (select.getGroupBy() != null || select.getHaving() != null) {
            throw unsupported("GROUP BY and HAVING are not supported");
        }
        if (select.getOrderByElements() != null && !select.getOrderByElements().isEmpty()) {
            throw unsupported("ORDER BY is not supported");
        }
        if (select.getLimit() != null || select.getOffset() != null || select.getFetch() != null
                || select.getTop() != null || select.getSkip() != null) {
            throw unsupported("pagination is not supported");
        }
        if (select.getDistinct() != null || select.isForUpdate()) {
            throw unsupported("DISTINCT and FOR UPDATE are not supported");
        }
    }

    private static Table requireTable(FromItem fromItem) {
        if (!(fromItem instanceof Table)) {
            throw unsupported("only one physical table is supported");
        }
        Table table = (Table) fromItem;
        if (table.getAlias() != null) {
            throw unsupported("table aliases are not supported");
        }
        return table;
    }

    private static Predicate parsePredicate(Expression expression, String tableName, RowType rowType,
                                            PredicateBuilder builder) {
        if (expression instanceof Parenthesis) {
            return parsePredicate(((Parenthesis) expression).getExpression(), tableName, rowType, builder);
        }
        if (expression instanceof AndExpression) {
            BinaryExpression binary = (BinaryExpression) expression;
            return PredicateBuilder.and(
                    parsePredicate(binary.getLeftExpression(), tableName, rowType, builder),
                    parsePredicate(binary.getRightExpression(), tableName, rowType, builder));
        }
        if (expression instanceof OrExpression) {
            BinaryExpression binary = (BinaryExpression) expression;
            return PredicateBuilder.or(
                    parsePredicate(binary.getLeftExpression(), tableName, rowType, builder),
                    parsePredicate(binary.getRightExpression(), tableName, rowType, builder));
        }
        if (expression instanceof IsNullExpression) {
            IsNullExpression isNull = (IsNullExpression) expression;
            int index = fieldIndex(isNull.getLeftExpression(), tableName, rowType, builder);
            return isNull.isNot() ? builder.isNotNull(index) : builder.isNull(index);
        }
        if (expression instanceof InExpression) {
            return parseIn((InExpression) expression, tableName, rowType, builder);
        }
        if (expression instanceof EqualsTo || expression instanceof NotEqualsTo
                || expression instanceof GreaterThan || expression instanceof GreaterThanEquals
                || expression instanceof MinorThan || expression instanceof MinorThanEquals) {
            return parseComparison((BinaryExpression) expression, tableName, rowType, builder);
        }
        throw unsupported("unsupported WHERE expression: " + expression);
    }

    private static Predicate parseComparison(BinaryExpression expression, String tableName,
                                             RowType rowType, PredicateBuilder builder) {
        int index = fieldIndex(expression.getLeftExpression(), tableName, rowType, builder);
        Object value = literal(expression.getRightExpression());
        value = PredicateBuilder.convertJavaObject(rowType.getTypeAt(index), value);
        if (expression instanceof EqualsTo) {
            return builder.equal(index, value);
        }
        if (expression instanceof NotEqualsTo) {
            return builder.notEqual(index, value);
        }
        if (expression instanceof GreaterThan) {
            return builder.greaterThan(index, value);
        }
        if (expression instanceof GreaterThanEquals) {
            return builder.greaterOrEqual(index, value);
        }
        if (expression instanceof MinorThan) {
            return builder.lessThan(index, value);
        }
        return builder.lessOrEqual(index, value);
    }

    private static Predicate parseIn(InExpression expression, String tableName, RowType rowType,
                                    PredicateBuilder builder) {
        int index = fieldIndex(expression.getLeftExpression(), tableName, rowType, builder);
        if (!(expression.getRightItemsList() instanceof ExpressionList)) {
            throw unsupported("IN subqueries are not supported");
        }
        List<Expression> expressions = ((ExpressionList) expression.getRightItemsList()).getExpressions();
        if (expressions == null || expressions.isEmpty()) {
            throw unsupported("empty IN lists are not supported");
        }
        List<Object> values = new ArrayList<>(expressions.size());
        for (Expression item : expressions) {
            values.add(PredicateBuilder.convertJavaObject(rowType.getTypeAt(index), literal(item)));
        }
        return expression.isNot() ? builder.notIn(index, values) : builder.in(index, values);
    }

    private static int fieldIndex(Expression expression, String tableName, RowType rowType,
                                  PredicateBuilder builder) {
        if (!(expression instanceof Column)) {
            throw unsupported("a predicate must compare a table column with a literal");
        }
        Column column = (Column) expression;
        Table qualifier = column.getTable();
        if (qualifier != null && qualifier.getName() != null
                && !qualifier.getName().equals(tableName)) {
            throw unsupported("column belongs to another table: " + column);
        }
        int index = builder.indexOf(column.getColumnName());
        if (index < 0) {
            throw unsupported("unknown column: " + column.getColumnName());
        }
        return index;
    }

    private static Object literal(Expression expression) {
        if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        }
        if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        }
        if (expression instanceof DoubleValue) {
            return ((DoubleValue) expression).getValue();
        }
        if (expression instanceof DateValue) {
            return ((DateValue) expression).getValue();
        }
        if (expression instanceof TimestampValue) {
            return ((TimestampValue) expression).getValue();
        }
        if (expression instanceof SignedExpression) {
            SignedExpression signed = (SignedExpression) expression;
            Object value = literal(signed.getExpression());
            if (signed.getSign() == '+') {
                return value;
            }
            if (value instanceof Long) {
                return -((Long) value);
            }
            if (value instanceof Double) {
                return -((Double) value);
            }
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).negate();
            }
        }
        if (expression instanceof NullValue) {
            throw unsupported("NULL is only supported by IS NULL or IS NOT NULL");
        }
        throw unsupported("unsupported literal: " + expression);
    }

    private static IllegalArgumentException unsupported(String message) {
        return new IllegalArgumentException("Unsupported Paimon SQL: " + message);
    }

    private static IllegalArgumentException unsupported(String message, Throwable cause) {
        return new IllegalArgumentException("Unsupported Paimon SQL: " + message, cause);
    }

    static final class ParsedQuery {
        private final String tableName;
        private final boolean countQuery;
        private final Predicate predicate;

        private ParsedQuery(String tableName, boolean countQuery, Predicate predicate) {
            this.tableName = tableName;
            this.countQuery = countQuery;
            this.predicate = predicate;
        }

        String tableName() {
            return tableName;
        }

        boolean countQuery() {
            return countQuery;
        }

        Predicate predicate() {
            return predicate;
        }
    }
}
