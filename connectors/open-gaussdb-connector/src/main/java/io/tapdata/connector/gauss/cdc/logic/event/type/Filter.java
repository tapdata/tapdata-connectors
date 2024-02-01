package io.tapdata.connector.gauss.cdc.logic.event.type;

import java.util.Set;

abstract class Filter {
    public abstract Object convert();

    public abstract boolean ifSupported(String typeName);

    public abstract Set<String> supportedType();
}