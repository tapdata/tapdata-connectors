package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;

import java.util.Objects;

/** Applies the effective operation to both InternalRow and Paimon's configured rowkind field. */
final class PaimonRowKindField {

    private PaimonRowKindField() {}

    static void apply(
            PaimonWriteSemanticContract contract, InternalRow row, RowKind effectiveRowKind) {
        Objects.requireNonNull(contract, "contract");
        Objects.requireNonNull(row, "row");
        Objects.requireNonNull(effectiveRowKind, "effectiveRowKind");
        row.setRowKind(effectiveRowKind);
        if (contract.rowKindFieldIndex() >= 0) {
            if (!(row instanceof GenericRow)) {
                throw new PaimonFatalWriteException(
                        "Configured Paimon rowkind field requires a mutable GenericRow");
            }
            ((GenericRow) row).setField(
                    contract.rowKindFieldIndex(),
                    BinaryString.fromString(effectiveRowKind.shortString()));
        }
    }
}
