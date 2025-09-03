package io.tapdata.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

class PostManAPIInvokerTest {
    PostManAPIInvoker invoker;

    @BeforeEach
    void init() {
        invoker = mock(PostManAPIInvoker.class);
    }

    @Nested
    class DoWaitByParamConfigTest {

        @BeforeEach
        void init() {
            doCallRealMethod().when(invoker).doWaitByParamConfig(anyMap());
        }

        @Test
        void testNotContainSleepConfigParam() {
            invoker.doWaitByParamConfig(new java.util.HashMap<>());
        }

        @Test
        void testSleepConfigParamNotNumber() {
            java.util.Map<String, Object> params = new java.util.HashMap<>();
            params.put(PostManAPIInvoker.SLEEP_CONFIG_PARAM_KEY, "1000");
            invoker.doWaitByParamConfig(params);
        }

        @Test
        void testSleepConfigParamLessThanZero() {
            java.util.Map<String, Object> params = new java.util.HashMap<>();
            params.put(PostManAPIInvoker.SLEEP_CONFIG_PARAM_KEY, -1);
            invoker.doWaitByParamConfig(params);
        }

        @Test
        void testSleepConfigParamMoreThanTenThousand() {
            java.util.Map<String, Object> params = new java.util.HashMap<>();
            params.put(PostManAPIInvoker.SLEEP_CONFIG_PARAM_KEY, 10001);
            invoker.doWaitByParamConfig(params);
        }

        @Test
        void testNormal() {
            java.util.Map<String, Object> params = new java.util.HashMap<>();
            params.put(PostManAPIInvoker.SLEEP_CONFIG_PARAM_KEY, 1L);
            invoker.doWaitByParamConfig(params);
        }
    }
}