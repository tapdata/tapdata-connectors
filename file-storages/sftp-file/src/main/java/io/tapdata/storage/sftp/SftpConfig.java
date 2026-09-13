package io.tapdata.storage.sftp;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.io.Serializable;
import java.util.Map;

public class SftpConfig implements Serializable {

    private String sftpHost;
    private int sftpPort = 22;
    private String sftpUsername;
    private String sftpPassword;
    private String encoding = "UTF-8";
    private String sftpStrictHostKeyChecking = "yes";
    private String sftpKnownHosts;
    private int sftpConnectionTimeoutMillis = 10000;

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    public SftpConfig load(Map<String, Object> params) {
        assert beanUtils != null;
        beanUtils.mapToBean(params, this);
        return this;
    }

    public String getSftpHost() {
        return sftpHost;
    }

    public void setSftpHost(String sftpHost) {
        this.sftpHost = sftpHost;
    }

    public int getSftpPort() {
        return sftpPort;
    }

    public void setSftpPort(int sftpPort) {
        this.sftpPort = sftpPort;
    }

    public String getSftpUsername() {
        return sftpUsername;
    }

    public void setSftpUsername(String sftpUsername) {
        this.sftpUsername = sftpUsername;
    }

    public String getSftpPassword() {
        return sftpPassword;
    }

    public void setSftpPassword(String sftpPassword) {
        this.sftpPassword = sftpPassword;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getSftpStrictHostKeyChecking() {
        return sftpStrictHostKeyChecking;
    }

    public void setSftpStrictHostKeyChecking(String sftpStrictHostKeyChecking) {
        this.sftpStrictHostKeyChecking = sftpStrictHostKeyChecking;
    }

    public String getSftpKnownHosts() {
        return sftpKnownHosts;
    }

    public void setSftpKnownHosts(String sftpKnownHosts) {
        this.sftpKnownHosts = sftpKnownHosts;
    }

    public int getSftpConnectionTimeoutMillis() {
        return sftpConnectionTimeoutMillis;
    }

    public void setSftpConnectionTimeoutMillis(int sftpConnectionTimeoutMillis) {
        this.sftpConnectionTimeoutMillis = sftpConnectionTimeoutMillis;
    }

    public void validate() {
        if (sftpHost == null || sftpHost.trim().isEmpty()) {
            throw new IllegalArgumentException("sftpHost is required");
        }
        if (sftpUsername == null || sftpUsername.trim().isEmpty()) {
            throw new IllegalArgumentException("sftpUsername is required");
        }
        if (sftpPort <= 0 || sftpPort > 65535) {
            throw new IllegalArgumentException("sftpPort is invalid");
        }
        if (encoding == null || encoding.trim().isEmpty()) {
            encoding = "UTF-8";
        }
        if (sftpStrictHostKeyChecking == null || sftpStrictHostKeyChecking.trim().isEmpty()) {
            sftpStrictHostKeyChecking = "yes";
        }
        if (!"yes".equalsIgnoreCase(sftpStrictHostKeyChecking)
                && !"no".equalsIgnoreCase(sftpStrictHostKeyChecking)
                && !"ask".equalsIgnoreCase(sftpStrictHostKeyChecking)) {
            throw new IllegalArgumentException("sftpStrictHostKeyChecking must be yes, no or ask");
        }
        if (sftpConnectionTimeoutMillis <= 0) {
            throw new IllegalArgumentException("sftpConnectionTimeoutMillis must be positive");
        }
    }
}
