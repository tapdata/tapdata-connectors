package io.tapdata.connector.paimon.perf;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据生成器 - 为 AutoTestRunner 专用
 * 生成交易明细表测试数据（29个字段）
 */
public class PaimonDataGenerator {
    private final long totalRecords;
    private final int warmupRecords;
    private final double duplicateRate;
    private final int batchSize;
    
    private final Random random;
    private final AtomicLong generatedCount;
    private final AtomicLong idCounter;
    
    // 交易明细表字段数据
    private static final String[] COMPOR_IDS = {"C001", "C002", "C003", "C004", "C005"};
    private static final String[] TXN_TYPES = {"SALE", "REFUND", "VOID", "EXCHANGE"};
    private static final String[] CHANNELS = {"CASH", "CARD", "MOBILE", "ONLINE"};
    private static final String[] OUTLETS = {"OUTLET_A", "OUTLET_B", "OUTLET_C", "OUTLET_D"};
    private static final String[] SOURCES = {"POS", "ECOM", "MOBILE_APP"};
    private static final String[] DOLLAR_TYPES = {"USD", "EUR", "CNY", "JPY"};
    private static final String[] PROPERTIES = {"NORMAL", "VIP", "PROMO"};

    public PaimonDataGenerator(long totalRecords, int warmupRecords, double duplicateRate, int batchSize) {
        this.totalRecords = totalRecords;
        this.warmupRecords = warmupRecords;
        this.duplicateRate = duplicateRate;
        this.batchSize = batchSize;
        this.random = new Random(42); // 固定种子以保证可重复性
        this.generatedCount = new AtomicLong(0);
        this.idCounter = new AtomicLong(1);
    }

    /**
     * 是否还有更多记录
     */
    public boolean hasMore() {
        return generatedCount.get() < totalRecords;
    }

    /**
     * 生成下一条记录
     */
    public InternalRow nextRecord() {
        if (!hasMore()) {
            return null;
        }

        long currentId = generatedCount.incrementAndGet();
        
        // 根据重复率决定是否生成重复ID
        long id;
        if (duplicateRate > 0 && generatedCount.get() > warmupRecords && random.nextDouble() < duplicateRate) {
            id = Math.max(1, idCounter.get() - random.nextInt(1000) - 1);
        } else {
            id = idCounter.getAndIncrement();
        }

        return createRow(id);
    }

    /**
     * 创建一行数据
     */
    private GenericRow createRow(long id) {
        GenericRow row = GenericRow.of(
            id,                                          // id (BIGINT)
            BinaryString.fromString("BD_" + id),        // balance_detail_id (STRING)
            Decimal.fromBigDecimal(randomBigDecimal(18, 0), 18, 0), // before_detail_balance (DECIMAL)
            Decimal.fromBigDecimal(randomBigDecimal(18, 0), 18, 0), // amount (DECIMAL)
            BinaryString.fromString(randomDateTime()),  // expiry_date (TIMESTAMP)
            BinaryString.fromString(randomArrayElement(COMPOR_IDS)), // compor_id (STRING)
            BinaryString.fromString(randomArrayElement(TXN_TYPES)), // transaction_type (STRING)
            BinaryString.fromString(randomArrayElement(CHANNELS)), // channel (STRING)
            BinaryString.fromString("POS_REF_" + id),   // pos_reference (STRING)
            BinaryString.fromString(randomArrayElement(OUTLETS)), // outlet (STRING)
            BinaryString.fromString("Remark for " + id), // remark (STRING)
            BinaryString.fromString(randomDateTime()),  // created_time (TIMESTAMP)
            BinaryString.fromString("PD_" + id),        // payment_detail_id (STRING)
            id + 1000000L,                              // payment_id (BIGINT)
            BinaryString.fromString("admin"),           // created_by (STRING)
            BinaryString.fromString("I"),               // op (STRING)
            Decimal.fromBigDecimal(randomBigDecimal(18, 0), 18, 0), // after_detail_balance (DECIMAL)
            BinaryString.fromString(randomArrayElement(SOURCES)), // source_system (STRING)
            BinaryString.fromString(randomArrayElement(DOLLAR_TYPES)), // dollar_type_id (STRING)
            Decimal.fromBigDecimal(randomBigDecimal(18, 0), 18, 0), // exception_balance (DECIMAL)
            BinaryString.fromString("PATRON_" + (id % 1000)), // patron_id (STRING)
            BinaryString.fromString("KEY_" + id),       // source_key (STRING)
            BinaryString.fromString("DEV_" + (id % 100)), // device_id (STRING)
            Decimal.fromBigDecimal(randomBigDecimal(18, 0), 18, 0), // after_balance (DECIMAL)
            Decimal.fromBigDecimal(randomBigDecimal(18, 0), 18, 0), // before_balance (DECIMAL)
            BinaryString.fromString(randomArrayElement(OUTLETS)), // outlet_code (STRING)
            BinaryString.fromString(randomDateTime()),  // ods_updated_at (TIMESTAMP)
            BinaryString.fromString(randomArrayElement(PROPERTIES)), // property (STRING)
            20240101 + (int)(id % 10000)                // pt_created_date (INT)
        );
        
        return row;
    }

    private BigDecimal randomBigDecimal(int precision, int scale) {
        long maxValue = (long) Math.pow(10, precision - scale) - 1;
        long randomValue = (long) (random.nextDouble() * maxValue);
        return BigDecimal.valueOf(randomValue, scale);
    }

    private String randomDateTime() {
        long epochDay = 19000 + random.nextInt(1000); // 2022-2025
        int secondOfDay = random.nextInt(86400);
        LocalDateTime ldt = LocalDateTime.ofEpochSecond(epochDay * 86400L + secondOfDay, 0, java.time.ZoneOffset.UTC);
        return ldt.toString();
    }

    private <T> T randomArrayElement(T[] array) {
        return array[random.nextInt(array.length)];
    }
}
