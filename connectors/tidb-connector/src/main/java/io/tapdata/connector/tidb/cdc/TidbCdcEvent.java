package io.tapdata.connector.tidb.cdc;


import org.tikv.kvproto.Cdcpb;

public class TidbCdcEvent {
    enum CDCEventType {
        ROW,
        RESOLVED_TS,
        ERROR
    }

    public final long regionId;

    public final CDCEventType eventType;

    public final long resolvedTs;

    public final Cdcpb.Event.Row row;

    public final Throwable error;

    private TidbCdcEvent(
            final long regionId,
            final CDCEventType eventType,
            final long resolvedTs,
            final Cdcpb.Event.Row row,
            final Throwable error) {
        this.regionId = regionId;
        this.eventType = eventType;
        this.resolvedTs = resolvedTs;
        this.row = row;
        this.error = error;
    }

    public static TidbCdcEvent rowEvent(final long regionId, final Cdcpb.Event.Row row) {
        return new TidbCdcEvent(regionId, CDCEventType.ROW, 0, row, null);
    }

    public static TidbCdcEvent resolvedTsEvent(final long regionId, final long resolvedTs) {
        return new TidbCdcEvent(regionId, CDCEventType.RESOLVED_TS, resolvedTs, null, null);
    }

    public static TidbCdcEvent errorMessage(final long regionId, final Throwable error) {
        return new TidbCdcEvent(regionId, CDCEventType.ERROR, 0, null, error);
    }

    public static TidbCdcEvent errorResolvedTs(final long regionId, final Throwable error, long resolvedTs) {
        return new TidbCdcEvent(regionId, CDCEventType.ERROR, resolvedTs, null, error);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("CDCEvent[").append(eventType.toString()).append("] {");
        switch (eventType) {
            case ERROR:
                builder.append("error=").append(error.getMessage());
                break;
            case RESOLVED_TS:
                builder.append("resolvedTs=").append(resolvedTs);
                break;
            case ROW:
                builder.append("row=").append(row);
                break;
        }
        return builder.append("}").toString();
    }
}
