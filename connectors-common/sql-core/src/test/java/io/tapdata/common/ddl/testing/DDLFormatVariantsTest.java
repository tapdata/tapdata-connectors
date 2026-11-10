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
}
