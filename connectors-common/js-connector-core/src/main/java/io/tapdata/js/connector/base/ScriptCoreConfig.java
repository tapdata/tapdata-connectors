package io.tapdata.js.connector.base;

public class ScriptCoreConfig {
    boolean ignoreReferenceTime;

    public boolean isIgnoreReferenceTime() {
        return ignoreReferenceTime;
    }

    public void setIgnoreReferenceTime(boolean ignoreReferenceTime) {
        this.ignoreReferenceTime = ignoreReferenceTime;
    }

    public ScriptCoreConfig ignoreReferenceTime(boolean needReferenceTime) {
        this.ignoreReferenceTime = needReferenceTime;
        return this;
    }
}
