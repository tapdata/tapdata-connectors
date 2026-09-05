package io.tapdata.common.entity;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class HashReadOffset implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer maxSplit;
    private Set<Integer> finishedSplits;
    /**
     * 本次运行 split 划分空间的指纹（例如 tdengine 按时间边界分片时记录数据范围），
     * 用于恢复时判断已完成的 split 是否仍然可信；null 表示无需校验（如按主键 hash 取模的固定分片）
     */
    private String fingerprint;

    public HashReadOffset() {
    }

    public HashReadOffset(final Integer maxSplit) {
        this(maxSplit, null);
    }

    public HashReadOffset(final Integer maxSplit, final String fingerprint) {
        this.maxSplit = maxSplit;
        this.fingerprint = fingerprint;
    }

    public Integer getMaxSplits() {
        return maxSplit;
    }

    public void setMaxSplit(final Integer maxSplit) {
        this.maxSplit = maxSplit;
    }

    public Set<Integer> getFinishedSplits() {
        return finishedSplits;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(final String fingerprint) {
        this.fingerprint = fingerprint;
    }

    public void addFinishedSplit(final Integer split) {
        if (finishedSplits == null) {
            finishedSplits = new ConcurrentSkipListSet<>();
        }
        finishedSplits.add(split);
    }

    public void setFinishedSplits(final Set<Integer> finishedSplits) {
        this.finishedSplits = finishedSplits;
    }
}
