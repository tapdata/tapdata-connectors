package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.paimon.catalog.*;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.table.source.*;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.data.InternalRow;

import java.math.BigDecimal;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaimonDecimalPredicateTest {
    @TempDir Path directory;
    private static final RowType TYPE = RowType.of(DataTypes.DECIMAL(38, 6));

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "= 60.123456 | 60.123456 | true",
        "= 60 | 60.000000 | true",
        "= -60.123456 | -60.123456 | true",
        "= +60.123456 | 60.123456 | true",
        "= 6.0123456E1 | 60.123456 | true",
        "= 9007199254740993.123456 | 9007199254740993.123456 | true",
        "= 18446744073709551615 | 18446744073709551615 | true",
        "= 9007199254740993.123456 | 9007199254740992.123456 | false",
        "<> 60.123456 | 60.123456 | false",
        "> 60.123455 | 60.123456 | true",
        ">= 60.123456 | 60.123456 | true",
        "< -60.123455 | -60.123456 | true",
        "<= -60.123456 | -60.123456 | true",
        "IN (0, 60.123456, -1.2) | 60.123456 | true",
        "NOT IN (0, 60.123456, -1.2) | 60.123456 | false",
        "BETWEEN -60.123456 AND 60.123456 | 60.123456 | true",
        "NOT BETWEEN -60.123456 AND 60.123456 | 61 | true"
    })
    void decimalPredicatesMustKeepLiteralDigits(String condition, String stored, boolean expected) {
        assertEquals(expected, PaimonSqlQueryParser.parse(
                "SELECT * FROM t WHERE f0 " + condition, TYPE).predicate().test(GenericRow.of(
                        Decimal.fromBigDecimal(new BigDecimal(stored), 38, 6))));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "= 1.2345678", "IN (1.2345678)", "BETWEEN 1 AND 1.2345678",
        "= 999999999999999999999999999999999999999", "= 1E100",
        "= 1E1000000000", "= 1E-1000000000"
    })
    void predicatesMustRejectUnrepresentableLiteralsInsteadOfRoundingOrReturningNull(String condition) {
        assertThrows(IllegalArgumentException.class, () -> PaimonSqlQueryParser.parse(
                "SELECT * FROM t WHERE f0 " + condition, TYPE));
    }

    @Test
    void exactDecimalPredicateMustWorkOnCommittedNativeTable() throws Exception {
        Options options = new Options();
        options.set("warehouse", directory.resolve("warehouse").toString());
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("default", true);
            Identifier id = Identifier.create("default", "decimal_values");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT())
                    .column("v", DataTypes.DECIMAL(38, 6)).primaryKey("id")
                    .option("bucket", "1").build(), false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(id);
            StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
            try (StreamTableWrite write = writeBuilder.newWrite();
                    StreamTableCommit commit = writeBuilder.newCommit()) {
                write.write(GenericRow.of(1, Decimal.fromBigDecimal(new BigDecimal("9007199254740993.123456"), 38, 6)));
                write.write(GenericRow.of(2, Decimal.fromBigDecimal(new BigDecimal("9007199254740992.123456"), 38, 6)));
                commit.commit(1L, write.prepareCommit(false, 1L));
            }
            ReadBuilder read = table.newReadBuilder().withFilter(PaimonSqlQueryParser.parse(
                    "SELECT * FROM decimal_values WHERE v = 9007199254740993.123456", table.rowType()).predicate());
            int matches = 0;
            for (Split split : read.newScan().plan().splits()) {
                try (RecordReader<InternalRow> reader = read.newRead().executeFilter().createReader(split)) {
                    RecordReader.RecordIterator<InternalRow> batch;
                    while ((batch = reader.readBatch()) != null) {
                        try {
                            InternalRow row;
                            while ((row = batch.next()) != null) {
                                assertEquals(1, row.getInt(0));
                                matches++;
                            }
                        } finally {
                            batch.releaseBatch();
                        }
                    }
                }
            }
            assertEquals(1, matches);
        }
    }
}
