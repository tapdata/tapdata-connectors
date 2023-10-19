package io.tapdata.connector.redis;

import java.io.Serializable;

public class RedisOffset implements Serializable {

    private String replId;
    private Long offsetV1;

    public RedisOffset() {
    }

    public RedisOffset(String replId, Long offsetV1) {
        this.replId = replId;
        this.offsetV1 = offsetV1;
    }

    public String getReplId() {
        return replId;
    }

    public void setReplId(String replId) {
        this.replId = replId;
    }

    public Long getOffsetV1() {
        return offsetV1;
    }

    public void setOffsetV1(Long offsetV1) {
        this.offsetV1 = offsetV1;
    }
}
