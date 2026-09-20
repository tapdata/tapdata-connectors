package io.tapdata.connector.paimon.service;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

/** 仅拆除故障注入测试拥有的静态登记；生产没有解冻/强制释放入口。 */
final class PaimonStopTestSupport {
    private PaimonStopTestSupport() {}
    @SuppressWarnings("unchecked")
    static void releaseInjectedOwner(PaimonService service, String table) throws Exception {
        // 调用者必须先证明测试创建的 executor 已退出；从不搜索/释放其他 service 的 owner。
        Map<String, String> physical = (Map<String, String>) field(service, "physicalTableByLogicalTable");
        String hash = physical.remove(table);
        if (hash != null) {
            ((Map<String, String>) field(service, "ACTIVE_PHYSICAL_TABLE_OWNERS"))
                    .remove(hash, field(service, "serviceWriterOwner") + ":" + table);
        }
        ((Set<String>) field(service, "unsafeResourceOwners")).remove(table);
    }
    private static Object field(Object owner, String name) throws Exception {
        Field field = owner.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(owner);
    }
}
