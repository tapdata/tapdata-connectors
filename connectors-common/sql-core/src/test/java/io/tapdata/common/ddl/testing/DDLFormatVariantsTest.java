package io.tapdata.common.ddl.testing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class DDLFormatVariantsTest {
    @Test
    void createsDeterministicCaseAndWhitespaceVariantsWithoutChangingLiterals() {
        String sql = "alter table orders add note varchar2(20) default 'Keep Case'";

        List<String> first = DDLFormatVariants.variants(sql);
        List<String> second = DDLFormatVariants.variants(sql);

        Assertions.assertEquals(first, second);
        Assertions.assertTrue(first.size() >= 10);
        Assertions.assertTrue(first.contains("ALTER TABLE ORDERS ADD NOTE VARCHAR2(20) DEFAULT 'Keep Case'"));
        Assertions.assertTrue(first.stream().anyMatch(variant -> variant.contains("\r\n")));
        Assertions.assertTrue(first.stream().anyMatch(variant -> variant.contains("\t")));
        Assertions.assertTrue(first.stream().anyMatch(variant -> variant.contains("/* ddl format */")));
        Assertions.assertTrue(first.stream().anyMatch(variant -> variant.contains("-- ddl format\n")));
        Assertions.assertTrue(first.stream().allMatch(variant -> variant.contains("'Keep Case'")));
    }

    @Test
    void insertsCommentsBeforeClauseKeywordsOutsideQuotedIdentifiersAndExistingComments() {
        assertCommentInsertedBeforeClause("ALTER TABLE \"x add y\" ADD c INT",
                "ALTER TABLE \"x add y\" /* ddl format */ ADD c INT");
        assertCommentInsertedBeforeClause("ALTER TABLE t /* add marker */ ADD c INT",
                "ALTER TABLE t /* add marker */ /* ddl format */ ADD c INT");
        assertCommentInsertedBeforeClause("ALTER TABLE t -- add marker\n ADD c INT",
                "ALTER TABLE t -- add marker\n /* ddl format */ ADD c INT");
    }

    private static void assertCommentInsertedBeforeClause(String sql, String expected) {
        List<String> variants = DDLFormatVariants.variants(sql);
        Assertions.assertTrue(variants.contains(expected), () -> "Missing expected variant: " + expected);
    }
}
