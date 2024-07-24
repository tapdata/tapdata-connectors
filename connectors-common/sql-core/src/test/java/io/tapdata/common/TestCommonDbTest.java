package io.tapdata.common;

import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.function.Consumer;

public class TestCommonDbTest {
    CommonDbTest commonDbTest = new CommonDbTest();
    @BeforeEach
    void init(){
        Consumer<TestItem> consumer = testItem -> {
        };
        ReflectionTestUtils.setField(commonDbTest,"consumer",consumer);
    }
    @Nested
    class TestTimeDifference{
        @Test
        void test(){
            Assertions.assertTrue(commonDbTest.testTimeDifference());
        }
    }
    @Nested
    class GetTimeDifferenceTest{
        @Test
        void test_main(){
            Assertions.assertTrue(commonDbTest.getTimeDifference(System.currentTimeMillis() + 2000) > 1000);
        }

        @Test
        void test_timeDifference_is_0(){
            Assertions.assertEquals(0L, commonDbTest.getTimeDifference(System.currentTimeMillis()));
        }

    }
}
