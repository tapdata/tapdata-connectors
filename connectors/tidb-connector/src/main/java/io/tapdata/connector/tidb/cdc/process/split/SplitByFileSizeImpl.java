package io.tapdata.connector.tidb.cdc.process.split;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SplitByFileSizeImpl implements SplitStage<File> {
    final int partitionBatch;
    public SplitByFileSizeImpl(int partitionBatch) {
        if (partitionBatch <= 0) {
            partitionBatch = 1;
        }
        this.partitionBatch = partitionBatch;
    }
    @Override
    public List<List<File>> splitToPieces(List<File> data, int eachPieceSize) {
        AtomicLong totalMemorySize = new AtomicLong();
        List<File> sortedFiles = data.stream().filter(Objects::nonNull).filter(file -> {
            totalMemorySize.addAndGet(file.length());
            return true;
        }).sorted((f1, f2) -> {
            long length1 = f1.length();
            long length2 = f2.length();
            return Long.compare(length1, length2);
        }).collect(Collectors.toList());
        long partitionSize = totalMemorySize.get() / partitionBatch;
        return foreachAllFiles(sortedFiles, partitionSize);
    }

    protected List<List<File>> foreachAllFiles(List<File> sortedFiles, long partitionSize) {
        List<List<File>> files = new ArrayList<>();
        Set<Integer> removedFiles = new HashSet<>();
        List<File> minSizeList = null;
        long minSize = 0;
        int location = 0;
        while (location != partitionBatch) {
            location ++;
            List<File> partition = new ArrayList<>();
            long currentSize = 0;
            int size = sortedFiles.size();
            for (int index = 0; index < size; index++) {
                if (removedFiles.contains(index)) {
                    continue;
                }
                File file = sortedFiles.get(index);
                String name = file.getName();
                long length = file.length();
                long expectedSize = currentSize + length;
                if (expectedSize > partitionSize) {
                    if (partition.isEmpty()) {
                        partition.add(file);
                        currentSize += length;
                        removedFiles.add(index);
                        break;
                    }
                    File lastAppropriateGoals = null;
                    Integer lastAppropriateGoalsIndex = null;
                    for (int indexReverse = size-1; indexReverse > index; indexReverse--) {
                        if (removedFiles.contains(index)) {
                            continue;
                        }
                        file = sortedFiles.get(indexReverse);
                        length = file.length();
                        expectedSize = currentSize + length;
                        if (expectedSize > partitionSize) {
                            if (null != lastAppropriateGoals) {
                                partition.add(file);
                                removedFiles.add(lastAppropriateGoalsIndex);
                            }
                            break;
                        }
                        if (expectedSize <= partitionSize) {
                            lastAppropriateGoals = file;
                            currentSize += length;
                            lastAppropriateGoalsIndex = indexReverse;
                        }
                        if (expectedSize == partitionSize) {
                            partition.add(file);
                            removedFiles.add(indexReverse);
                            break;
                        }
                    }
                    continue;
                }

                removedFiles.add(index);
                partition.add(file);
                currentSize += length;
                if (expectedSize == partitionSize) {
                    break;
                }
            }
            files.add(partition);
            if (null == minSizeList || minSize > currentSize) {
                minSize = currentSize;
                minSizeList = partition;
            }
        }
        if (removedFiles.size() != sortedFiles.size()) {
            for (int index = 0; index < sortedFiles.size(); index++) {
                if (!removedFiles.contains(index)) {
                    assert minSizeList != null;
                    minSizeList.add(sortedFiles.get(index));
                }
            }
        }
        return files;
    }
}
