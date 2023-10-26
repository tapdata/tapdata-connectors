package com.github.shyiko.mysql.binlog.event;

/**
 * MariaDB and MySQL have different GTID implementations, and that these are not compatible with each other.
 *
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 * @see <a href="https://mariadb.com/kb/en/gtid_event/">GTID_EVENT</a> for the original doc
 */
public class MariadbGtidEventData implements EventData {
    public static int FL_STANDALONE = 1;
    public static int FL_GROUP_COMMIT_ID = 2;
    public static int FL_TRANSACTIONAL = 4;
    public static int FL_ALLOW_PARALLEL = 8;
    public static int FL_WAITED = 16;
    public static int FL_DDL = 32;

    private long sequence;
    private long domainId;
    private long serverId;

    private int flags;

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public long getDomainId() {
        return domainId;
    }

    public void setDomainId(long domainId) {
        this.domainId = domainId;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    @Override
    public String toString() {
        return domainId + "-" + serverId + "-" + sequence;
    }
}
