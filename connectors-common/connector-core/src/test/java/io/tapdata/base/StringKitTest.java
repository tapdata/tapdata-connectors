package io.tapdata.base;

import io.tapdata.kit.StringKit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Nested
public class StringKitTest {

    @Test
    void testRemoveParentheses() {
        String result = StringKit.removeParentheses("VARCHAR2(100)");
        assertEquals("VARCHAR2", result);
        result = StringKit.removeParentheses("TIMESTAMP(6) WITH TIME ZONE");
        assertEquals("TIMESTAMP WITH TIME ZONE", result);
    }

    @Test
    void testRemoveSqlNote() {
        String result = StringKit.removeSqlNote("-----注释\n" +
                "alter table\n" +
                "    ----加注释\n" +
                "    xxx add /*又来注释*/ aaa int;\n");
        assertEquals("alter table\n" +
                "        xxx add  aaa int;", result);
    }
}
