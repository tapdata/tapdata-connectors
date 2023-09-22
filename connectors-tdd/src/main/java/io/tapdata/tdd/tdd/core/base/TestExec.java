package io.tapdata.tdd.tdd.core.base;

import java.lang.reflect.Method;

public interface TestExec {
    public void exec(TestNode prepare, Method testCase);
}