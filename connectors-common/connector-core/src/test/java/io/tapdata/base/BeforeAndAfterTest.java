package io.tapdata.base;

import io.tapdata.kit.DbKit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

class BeforeAndAfterTest {

    private static final List<String> COMMON_UNIQUE_CONDITION = Arrays.asList("id1", "id2");
    private static final List<String> COMMON_ALL_COLUMN = Arrays.asList("id1", "id2", "name", "age", "birth");
    private static final List<String> NONE_UNIQUE_CONDITION = Collections.emptyList();

    private static void assertBeforeAndAfter(List<String> allColumns, List<String> uniqueCondition, Map<String, Object> after, Map<String, Object> before, Map<String, Object> lastAfter, Map<String, Object> lastBefore) {
        Assertions.assertEquals(lastBefore, DbKit.getBeforeForUpdate(after, before, allColumns, uniqueCondition));
        Assertions.assertEquals(lastAfter, DbKit.getAfterForUpdate(after, before, allColumns, uniqueCondition));
    }

    /**
     * 0、most often
     */
    @Test
    void testMostOften() {
        Map<String, Object> after = map(
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> before = map(
                entry("id1", 1),
                entry("id2", 2)
        );
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 2)
        );
        Map<String, Object> lastAfter = map(
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        assertBeforeAndAfter(COMMON_ALL_COLUMN, COMMON_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }

    /**
     * 1、some datasource has no before data, so we should use after data as before data
     */
    @Test
    void testNullBefore() {
        Map<String, Object> after = map(
                entry("id1", 1),
                entry("id2", 2),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> before = null;
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 2)
        );
        Map<String, Object> lastAfter = map(
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        assertBeforeAndAfter(COMMON_ALL_COLUMN, COMMON_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }

    /**
     * 2、just update name from sam to jarad
     */
    @Test
    void testCommonBefore1() {
        Map<String, Object> after = map(
                entry("id1", 1),
                entry("id2", 2),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> before = map(
                entry("id1", 1),
                entry("id2", 2),
                entry("name", "sam"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 2)
        );
        Map<String, Object> lastAfter = map(
                entry("name", "jarad")
        );
        assertBeforeAndAfter(COMMON_ALL_COLUMN, COMMON_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }

    /**
     * 3、update name from sam to jarad, age 28 to 18
     */
    @Test
    void testCommonBefore2() {
        Map<String, Object> after = map(
                entry("id1", 1),
                entry("id2", 2),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> before = map(
                entry("id1", 1),
                entry("id2", 2),
                entry("name", "sam"),
                entry("age", 28),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 2)
        );
        Map<String, Object> lastAfter = map(
                entry("name", "jarad"),
                entry("age", 18)
        );
        assertBeforeAndAfter(COMMON_ALL_COLUMN, COMMON_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }

    /**
     * 4、some datasource has incomplete before data, so we should use after data as before data
     */
    @Test
    void testIncompleteBefore() {
        Map<String, Object> after = map(
                entry("id1", 1),
                entry("id2", 2),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> before = map(
                entry("id1", 1),
                entry("age", 18)
        );
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 2)
        );
        Map<String, Object> lastAfter = map(
                entry("name", "jarad"),
                entry("birth", "2022-05-29")
        );
        assertBeforeAndAfter(COMMON_ALL_COLUMN, COMMON_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }

    /**
     * 5、change unique condition
     */
    @Test
    void testChangeUniqueCondition() {
        Map<String, Object> after = map(
                entry("id1", 2),
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> before = map(
                entry("id1", 1),
                entry("id2", 2),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 2)
        );
        Map<String, Object> lastAfter = map(
                entry("id1", 2),
                entry("id2", 3)
        );
        assertBeforeAndAfter(COMMON_ALL_COLUMN, COMMON_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }

    /**
     * 6、change part of unique condition and part of data
     */
    @Test
    void testChangePartUniqueCondition() {
        Map<String, Object> after = map(
                entry("id1", 1),
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> before = map(
                entry("id1", 1),
                entry("id2", 2),
                entry("name", "sam"),
                entry("age", 28),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 2)
        );
        Map<String, Object> lastAfter = map(
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18)
        );
        assertBeforeAndAfter(COMMON_ALL_COLUMN, COMMON_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }

    /**
     * 7、no unique condition
     */
    @Test
    void testNoUniqueCondition() {
        Map<String, Object> after = map(
                entry("id1", 1),
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> before = map(
                entry("id1", 1),
                entry("id2", 2),
                entry("name", "sam"),
                entry("age", 28),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 2),
                entry("name", "sam"),
                entry("age", 28),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> lastAfter = map(
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18)
        );
        assertBeforeAndAfter(COMMON_ALL_COLUMN, NONE_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }

    /**
     * 8、no unique condition and no before (invalid)
     */
    @Test
    void testNoUniqueConditionAndNoBefore() {
        Map<String, Object> after = map(
                entry("id1", 1),
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> before = null;
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> lastAfter = map(
                entry("id1", 1),
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        assertBeforeAndAfter(COMMON_ALL_COLUMN, NONE_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }

    /**
     * 9、after and before is the same (invalid)
     */
    @Test
    void testSameBeforeAndAfter() {
        Map<String, Object> after = map(
                entry("id1", 1),
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> before = map(
                entry("id1", 1),
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 3)
        );
        Map<String, Object> lastAfter = map(
                entry("id1", 1),
                entry("id2", 3),
                entry("name", "jarad"),
                entry("age", 18),
                entry("birth", "2022-05-29")
        );
        assertBeforeAndAfter(COMMON_ALL_COLUMN, COMMON_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }

    @Test
    void testAfterContainsNull() {
        Map<String, Object> after = map(
                entry("id1", 1),
                entry("id2", 3),
                entry("name", "jarad"),
                entry("birth", "2022-05-29")
        );
        after.put("age", null);
        Map<String, Object> before = null;
        Map<String, Object> lastBefore = map(
                entry("id1", 1),
                entry("id2", 3)
        );
        Map<String, Object> lastAfter = map(
                entry("name", "jarad"),
                entry("birth", "2022-05-29")
        );
        lastAfter.put("age", null);
        assertBeforeAndAfter(COMMON_ALL_COLUMN, COMMON_UNIQUE_CONDITION, after, before, lastAfter, lastBefore);
    }
}
