package io.tapdata.common;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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


}
