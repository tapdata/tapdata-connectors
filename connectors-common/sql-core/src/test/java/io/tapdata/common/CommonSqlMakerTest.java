package io.tapdata.common;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CommonSqlMakerTest {


    @Test
    void buildCommandWhereSqlFilterIsnullTest(){
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        String defaultWhereSql = "where id<0";
        String actualData = commonSqlMaker.buildCommandWhereSql(null,defaultWhereSql);
        Assertions.assertEquals(defaultWhereSql,actualData);
    }

    @Test
    void buildCommandWhereSqlFilterMatchisNullTest(){
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        TapAdvanceFilter filter = new TapAdvanceFilter();
        String defaultWhereSql = "where id<0";
        String actualData = commonSqlMaker.buildCommandWhereSql(filter,defaultWhereSql);
        Assertions.assertEquals(defaultWhereSql,actualData);
    }


    @Test
    void buildCommandWhereSqlParamsisNullTest(){
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        TapAdvanceFilter filter = new TapAdvanceFilter();
        DataMap match = new DataMap();
        match.put("customCommand",new HashMap<>());
        filter.setMatch(match);
        String defaultWhereSql = "where id<0";
        String actualData = commonSqlMaker.buildCommandWhereSql(filter,defaultWhereSql);
        Assertions.assertEquals(defaultWhereSql,actualData);
    }

    @Test
    void buildCommandWhereSqlParamsNoWhereTest(){
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        TapAdvanceFilter filter = new TapAdvanceFilter();
        DataMap match = new DataMap();
        Map customCommand = new HashMap();
        Map param = new HashMap<>();
        String whereSql = "id>1";
        param.put("sql",whereSql);
        customCommand.put("params",param);
        match.put("customCommand",customCommand);
        filter.setMatch(match);
        String defaultWhereSql = "where id<0";
        String actualData = commonSqlMaker.buildCommandWhereSql(filter,defaultWhereSql);
        Assertions.assertEquals("where "+whereSql,actualData);
    }


    @Test
    void buildCommandWhereSqlParamsWithWhereTest(){
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        TapAdvanceFilter filter = new TapAdvanceFilter();
        DataMap match = new DataMap();
        Map customCommand = new HashMap();
        Map param = new HashMap<>();
        String whereSql = "where id>1";
        param.put("sql",whereSql);
        customCommand.put("params",param);
        match.put("customCommand",customCommand);
        filter.setMatch(match);
        String defaultWhereSql = "where id<0";
        String actualData = commonSqlMaker.buildCommandWhereSql(filter,defaultWhereSql);
        Assertions.assertEquals(whereSql,actualData);
    }

    @Test
    void buildSqlByAdvanceFilterWithOffsetFetchTest() {
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        TapAdvanceFilter filter = new TapAdvanceFilter();

        // Test with both skip and limit
        filter.setSkip(10);
        filter.setLimit(20);
        filter.setSortOnList(Arrays.asList(new SortOn("id", SortOn.ASCENDING)));

        String result = commonSqlMaker.buildSqlByAdvanceFilterWithOffsetFetch(filter);
        String expected = " ORDER BY \"id\" ASC  OFFSET 10 ROWS  FETCH FIRST 20 ROWS ONLY ";
        Assertions.assertEquals(expected, result);
    }

    @Test
    void buildSqlByAdvanceFilterWithOffsetFetchOnlyLimitTest() {
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        TapAdvanceFilter filter = new TapAdvanceFilter();

        // Test with only limit (should add OFFSET 0)
        filter.setLimit(15);
        filter.setSortOnList(Arrays.asList(new SortOn("name", SortOn.DESCENDING)));

        String result = commonSqlMaker.buildSqlByAdvanceFilterWithOffsetFetch(filter);
        String expected = " ORDER BY \"name\" DESC  OFFSET 0 ROWS  FETCH FIRST 15 ROWS ONLY ";
        Assertions.assertEquals(expected, result);
    }

    @Test
    void buildSqlByAdvanceFilterWithOffsetFetchOnlySkipTest() {
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        TapAdvanceFilter filter = new TapAdvanceFilter();

        // Test with only skip
        filter.setSkip(5);
        filter.setSortOnList(Arrays.asList(new SortOn("created_at", SortOn.ASCENDING)));

        String result = commonSqlMaker.buildSqlByAdvanceFilterWithOffsetFetch(filter);
        String expected = " ORDER BY \"created_at\" ASC  OFFSET 5 ROWS ";
        Assertions.assertEquals(expected, result);
    }

    @Test
    void buildSqlByAdvanceFilterWithOffsetFetchNoOrderByTest() {
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        TapAdvanceFilter filter = new TapAdvanceFilter();

        // Test without ORDER BY but with pagination (should not add ORDER BY)
        filter.setSkip(10);
        filter.setLimit(20);

        String result = commonSqlMaker.buildSqlByAdvanceFilterWithOffsetFetch(filter);
        String expected = " OFFSET 10 ROWS  FETCH FIRST 20 ROWS ONLY ";
        Assertions.assertEquals(expected, result);
    }

    @Test
    void buildOffsetFetchClauseTest() {
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        StringBuilder builder = new StringBuilder();
        TapAdvanceFilter filter = new TapAdvanceFilter();

        filter.setSkip(100);
        filter.setLimit(50);
        filter.setSortOnList(Arrays.asList(new SortOn("id", SortOn.ASCENDING)));

        commonSqlMaker.buildOffsetFetchClause(builder, filter);
        String result = builder.toString();
        String expected = " OFFSET 100 ROWS  FETCH FIRST 50 ROWS ONLY ";
        Assertions.assertEquals(expected, result);
    }

    @Test
    void buildSqlByAdvanceFilterWithOffsetFetchNoPaginationTest() {
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        TapAdvanceFilter filter = new TapAdvanceFilter();

        // Test without any pagination parameters
        filter.setSortOnList(Arrays.asList(new SortOn("id", SortOn.ASCENDING)));

        String result = commonSqlMaker.buildSqlByAdvanceFilterWithOffsetFetch(filter);
        String expected = " ORDER BY \"id\" ASC ";
        Assertions.assertEquals(expected, result);
    }

    @Test
    void buildSqlByAdvanceFilterWithOffsetFetchEmptyTest() {
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker();
        TapAdvanceFilter filter = new TapAdvanceFilter();

        // Test with completely empty filter
        String result = commonSqlMaker.buildSqlByAdvanceFilterWithOffsetFetch(filter);
        String expected = "";
        Assertions.assertEquals(expected, result);
    }


}
