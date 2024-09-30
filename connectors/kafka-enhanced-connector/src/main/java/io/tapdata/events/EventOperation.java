package io.tapdata.events;

/**
 * 事件操作项
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/2 15:58 Create
 */
public enum EventOperation {
    DML_INSERT(EventOperationType.DML, "INSERT"),
    DML_UPDATE(EventOperationType.DML, "UPDATE"),
    DML_DELETE(EventOperationType.DML, "DELETE"),
    DML_UNKNOWN(EventOperationType.DML, "UNKNOWN"),

    DDL_CREATE_TABLE(EventOperationType.DDL, "CREATE_TABLE"),
    DDL_CREATE_VIEW(EventOperationType.DDL, "CREATE_VIEW"),
    DDL_CREATE_FUNCTION(EventOperationType.DDL, "CREATE_FUNCTION"),
    DDL_CREATE_PROCEDURE(EventOperationType.DDL, "CREATE_PROCEDURE"),
    DDL_CREATE_INDEX(EventOperationType.DDL, "CREATE_INDEX"),
    DDL_UNKNOWN(EventOperationType.DDL, "UNKNOWN"),
    ;

    private final EventOperationType type;
    private final String op;

    EventOperation(EventOperationType type, String op) {
        this.type = type;
        this.op = op;
    }

    public EventOperationType getType() {
        return type;
    }

    public boolean isType(String type) {
        return this.type.name().equals(type);
    }

    public boolean isType(EventOperationType type) {
        return this.type == type;
    }

    public String getOp() {
        return op;
    }

    public boolean isOp(String op) {
        return this.op.equals(op);
    }

    @Override
    public String toString() {
        return String.format("%s:%s", type, op);
    }

    public static EventOperation fromString(String op) {
        for (EventOperation current : EventOperation.values()) {
            if (current.toString().equals(op)) {
                return current;
            }
        }
        return null;
    }
}
