package io.tapdata.tdd.tdd.core.base;

import java.lang.reflect.Method;

public interface TestStart {
    public void start(TestNode prepare, Method testCase);
}