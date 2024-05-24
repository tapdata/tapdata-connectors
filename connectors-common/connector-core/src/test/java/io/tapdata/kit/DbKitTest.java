package io.tapdata.kit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class DbKitTest {

    @Test
    public void testGetAfterForUpdateWithEmptyBefore() {
        Map<String, Object> after = new HashMap<>();
        after.put("column1", "value1");
        after.put("column2", "value2");

        Map<String, Object> before = new HashMap<>();

        Collection<String> allColumns = new ArrayList<>(after.keySet());
        Collection<String> uniqueCondition = new ArrayList<>();

        Map<String, Object> result = DbKit.getAfterForUpdate(after, before, allColumns, uniqueCondition);

        Assertions.assertEquals(after, result);
    }

    @Test
    public void testGetAfterForUpdateWithNonEmptyBefore() {
        Map<String, Object> after = new HashMap<>();
        after.put("column1", "value1");
        after.put("column2", "value2");

        Map<String, Object> before = new HashMap<>();
        before.put("column1", "value1");

        Collection<String> allColumns = new ArrayList<>(after.keySet());
        Collection<String> uniqueCondition = new ArrayList<>();
        uniqueCondition.add("column1");
        Map<String, Object> result = DbKit.getAfterForUpdate(after, before, allColumns, uniqueCondition);

        Map<String, Object> expected = new HashMap<>(after);
        expected.remove("column1");

        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testGetAfterForUpdateWithUniqueCondition() {
        Map<String, Object> after = new HashMap<>();
        after.put("id", 1);
        after.put("column1", "newValue");

        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        before.put("column1", "oldValue");

        Collection<String> allColumns = new ArrayList<>(after.keySet());
        Collection<String> uniqueCondition = Collections.singletonList("id");

        Map<String, Object> result = DbKit.getAfterForUpdate(after, before, allColumns, uniqueCondition);

        Map<String, Object> expected = new HashMap<>(after);
        expected.remove("id");

        Assertions.assertEquals(expected, result);
    }

}

