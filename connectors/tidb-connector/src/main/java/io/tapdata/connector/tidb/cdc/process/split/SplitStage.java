package io.tapdata.connector.tidb.cdc.process.split;

import java.util.List;

public interface SplitStage<T> {
    List<List<T>> splitToPieces(List<T> data, int eachPieceSize);
}
