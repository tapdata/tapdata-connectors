package io.tapdata.connector.redis.bean;

import redis.clients.jedis.HostAndPort;

import java.util.ArrayList;
import java.util.List;

public class ClusterNode {

    private HostAndPort master;
    private List<HostAndPort> slaves;
    private Long beginSlot;
    private Long endSlot;

    public ClusterNode() {
    }

    public ClusterNode(HostAndPort master, Long beginSlot, Long endSlot) {
        this.master = master;
        this.beginSlot = beginSlot;
        this.endSlot = endSlot;
    }

    public void addSlave(HostAndPort slave) {
        if (this.slaves == null) {
            this.slaves = new ArrayList<>();
        }
        this.slaves.add(slave);
    }

    public boolean contains(HostAndPort node) {
        if (this.master.equals(node)) {
            return true;
        }
        if (this.slaves != null) {
            for (HostAndPort slave : this.slaves) {
                if (slave.equals(node)) {
                    return true;
                }
            }
        }
        return false;
    }

    public HostAndPort getMaster() {
        return master;
    }

    public void setMaster(HostAndPort master) {
        this.master = master;
    }

    public List<HostAndPort> getSlaves() {
        return slaves;
    }

    public void setSlaves(List<HostAndPort> slaves) {
        this.slaves = slaves;
    }

    public Long getBeginSlot() {
        return beginSlot;
    }

    public void setBeginSlot(Long beginSlot) {
        this.beginSlot = beginSlot;
    }

    public Long getEndSlot() {
        return endSlot;
    }

    public void setEndSlot(Long endSlot) {
        this.endSlot = endSlot;
    }
}
