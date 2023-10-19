package io.tapdata.tdd.tdd.core.base;

import java.lang.reflect.Method;

public interface TestStop {
    public void stop(TestNode prepare, Method testCase);
}