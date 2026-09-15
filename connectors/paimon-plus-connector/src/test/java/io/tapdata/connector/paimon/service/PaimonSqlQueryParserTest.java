package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonSqlQueryParserTest {

    private static final RowType ROW_TYPE = RowType.of(
            new DataType[]{DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()},
            new String[]{"id", "category", "nullable_value"});

    @Test
    void parsesSupportedWherePredicatesIntoExecutablePaimonPredicate() {
        PaimonSqlQueryParser.ParsedQuery query = PaimonSqlQueryParser.parse(
                "SELECT * FROM orders WHERE category = 'A' AND id >= 2", ROW_TYPE);

        Predicate predicate = query.predicate();
        assertEquals("orders", query.tableName());
        assertFalse(query.countQuery());
        assertTrue(predicate.test(GenericRow.of(2, BinaryString.fromString("A"), null)));
        assertFalse(predicate.test(GenericRow.of(1, BinaryString.fromString("A"), null)));
        assertFalse(predicate.test(GenericRow.of(2, BinaryString.fromString("B"), null)));
    }

    @Test
    void parsesCountWrapperAndNullPredicate() {
        PaimonSqlQueryParser.ParsedQuery query = PaimonSqlQueryParser.parse(
                "SELECT COUNT(1) FROM (SELECT * FROM orders WHERE nullable_value IS NULL) AS COUNT",
                ROW_TYPE);

        assertEquals("orders", query.tableName());
        assertTrue(query.countQuery());
        assertTrue(query.predicate().test(GenericRow.of(1, BinaryString.fromString("A"), null)));
        assertFalse(query.predicate().test(GenericRow.of(
                1, BinaryString.fromString("A"), BinaryString.fromString("value"))));
    }

    @Test
    void parsesOrInAndNotInPredicates() {
        PaimonSqlQueryParser.ParsedQuery query = PaimonSqlQueryParser.parse(
                "SELECT * FROM orders WHERE id IN (1, 3) OR category NOT IN ('B')", ROW_TYPE);

        assertTrue(query.predicate().test(GenericRow.of(1, BinaryString.fromString("B"), null)));
        assertTrue(query.predicate().test(GenericRow.of(2, BinaryString.fromString("A"), null)));
        assertFalse(query.predicate().test(GenericRow.of(2, BinaryString.fromString("B"), null)));
    }

    @Test
    void rejectsStatementsThatCouldMutateOrSilentlyScanTheWholeTable() {
        Stream.of(
                "SELECT * FROM orders",
                "INSERT INTO orders VALUES (1, 'A', NULL)",
                "DELETE FROM orders WHERE id = 1",
                "SELECT * FROM orders JOIN other ON orders.id = other.id WHERE orders.id = 1",
                "SELECT * FROM orders WHERE id BETWEEN 1 AND 2",
                "SELECT * FROM orders WHERE id = 1 ORDER BY id",
                "SELECT * FROM orders WHERE id = 1 LIMIT 1",
                "SELECT * FROM orders WHERE id = (SELECT id FROM other)",
                "CALL refresh_table('orders')",
                "CREATE TABLE other (id INT)")
                .forEach(sql -> assertThrows(IllegalArgumentException.class,
                        () -> PaimonSqlQueryParser.parse(sql, ROW_TYPE), sql));
    }
}
