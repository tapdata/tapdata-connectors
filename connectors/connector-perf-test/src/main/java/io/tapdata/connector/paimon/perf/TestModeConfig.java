package io.tapdata.connector.paimon.perf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 测试模式统一配置
 * 
 * <p>集中定义所有测试模式的定义、别名、描述和映射关系
 * 确保 Java 代码和 Shell 脚本使用同一份配置源
 * 
 * <p>新增测试模式只需在此类中添加一个 TestModeEntry 即可
 */
public class TestModeConfig {

    /**
     * 测试模式条目
     */
    public static class TestModeEntry {
        private final String id;              // 数字编号（用于交互式菜单）
        private final String primaryAlias;    // 主别名（用于命令行参数）
        private final String groupKey;        // 对应的 TestCase 组 key
        private final String description;     // 描述信息
        private final String testCaseRange;   // 测试用例范围说明

        public TestModeEntry(String id, String primaryAlias, String groupKey, 
                            String description, String testCaseRange) {
            this.id = id;
            this.primaryAlias = primaryAlias;
            this.groupKey = groupKey;
            this.description = description;
            this.testCaseRange = testCaseRange;
        }

        public String getId() { return id; }
        public String getPrimaryAlias() { return primaryAlias; }
        public String getGroupKey() { return groupKey; }
        public String getDescription() { return description; }
        public String getTestCaseRange() { return testCaseRange; }

        /**
         * 获取所有可用的别名（包括 id 和 primaryAlias）
         */
        public List<String> getAllAliases() {
            List<String> aliases = new ArrayList<>();
            aliases.add(id);
            aliases.add(primaryAlias);
            return Collections.unmodifiableList(aliases);
        }

        @Override
        public String toString() {
            return String.format("%s  %-14s - %s (%s)", id, primaryAlias, description, testCaseRange);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  统一测试模式配置列表
    //  新增测试模式请在此处添加，无需修改其他地方
    // ═══════════════════════════════════════════════════════════════════════

    /** 所有测试模式定义 */
    public static final List<TestModeEntry> ALL_MODES;
    
    static {
        List<TestModeEntry> modes = new ArrayList<>();
        
        // 基础测试
        modes.add(new TestModeEntry(
            "1", "basic", "basic",
            "基础用例组", "TC-01~03"
        ));
        
        // 全量测试
        modes.add(new TestModeEntry(
            "2", "all", "all",
            "全量测试(所有组)", "所有用例"
        ));
        
        // 无小文件测试
        modes.add(new TestModeEntry(
            "3", "nosmallfile", "nosmallfile",
            "无小文件测试", "TC-50~54"
        ));
        
        // 单个基准用例
        modes.add(new TestModeEntry(
            "4", "single", "single",
            "单个基准用例", "TC-01"
        ));
        
        // 分桶策略测试
        modes.add(new TestModeEntry(
            "5", "bucket", "bucket",
            "分桶策略测试", "TC-30~35"
        ));
        
        // 合并策略测试
        modes.add(new TestModeEntry(
            "6", "compaction", "compaction",
            "合并策略测试", "TC-40~45"
        ));
        
        // 写入缓冲区测试
        modes.add(new TestModeEntry(
            "7", "buffer", "buffer",
            "写入缓冲区测试", "TC-10~16"
        ));
        
        // 目标文件大小测试
        modes.add(new TestModeEntry(
            "8", "target", "target",
            "目标文件大小测试", "TC-20~23"
        ));
        
        // 文件格式压缩测试
        modes.add(new TestModeEntry(
            "9", "format", "format",
            "文件格式压缩测试", "TC-60~64"
        ));
        
        // 主键更新测试
        modes.add(new TestModeEntry(
            "10", "pkupdate", "pkupdate",
            "主键更新测试", "TC-70~73"
        ));
        
        // 并行度测试
        modes.add(new TestModeEntry(
            "11", "parallelism", "parallelism",
            "写入并行度测试", "TC-80~83"
        ));
        
        ALL_MODES = Collections.unmodifiableList(modes);
    }

    /**
     * 特殊模式：自动模式（无需交互，全自动运行）
     */
    public static final String AUTO_MODE_KEY = "auto";

    /**
     * 默认模式（用户未指定时使用）
     */
    public static final String DEFAULT_MODE_ID = "1";
    public static final String DEFAULT_MODE_ALIAS = "basic";
    public static final String DEFAULT_MODE_GROUP = "basic";

    /**
     * 根据输入（id 或别名）查找对应的组 key
     * 
     * @param input 用户输入（如 "1", "basic", "nosmallfile"）
     * @return 组 key（如 "basic", "nosmallfile"），如果未找到返回 null
     */
    public static String resolveToGroupKey(String input) {
        if (input == null || input.trim().isEmpty()) {
            return null;
        }
        
        String normalized = input.trim().toLowerCase();
        
        // 特殊处理 auto 模式
        if (AUTO_MODE_KEY.equals(normalized)) {
            return AUTO_MODE_KEY; // auto 模式默认运行全量
        }

        // 遍历所有模式查找匹配
        for (TestModeEntry mode : ALL_MODES) {
            if (mode.getId().equals(normalized) || 
                mode.getPrimaryAlias().equals(normalized)) {
                return mode.getGroupKey();
            }
        }
        
        return null; // 未找到
    }

    /**
     * 获取交互式菜单的显示文本
     * 
     * @return 菜单文本（多行）
     */
    public static String getInteractiveMenuText() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n  请选择测试模式（直接回车默认 ").append(DEFAULT_MODE_ID).append("）:\n");
        
        for (TestModeEntry mode : ALL_MODES) {
            sb.append("    ").append(mode.toString()).append("\n");
        }
        
        sb.append("    auto            - 全自动运行(无需交互)\n");
        sb.append("\n  > ");
        
        return sb.toString();
    }

    /**
     * 获取 Shell 脚本用的菜单文本（用于 run-perf-test.sh）
     * 
     * @return 菜单文本（多行）
     */
    public static String getShellMenuText() {
        StringBuilder sb = new StringBuilder();
        sb.append("请选择测试模式 (直接回车默认 ").append(DEFAULT_MODE_ID).append("):\n");
        
        for (TestModeEntry mode : ALL_MODES) {
            sb.append("  ").append(mode.toString()).append("\n");
        }
        
        sb.append("  auto            - 全自动运行(无需按回车)\n");
        
        return sb.toString();
    }

    /**
     * 获取所有可用的命令行参数别名
     * 用于 Shell 脚本的参数验证和提示
     * 
     * @return 别名字符串（空格分隔）
     */
    public static String getAllAliasesForShell() {
        StringBuilder sb = new StringBuilder();
        for (TestModeEntry mode : ALL_MODES) {
            sb.append(mode.getPrimaryAlias()).append(" ");
        }
        sb.append(AUTO_MODE_KEY);
        return sb.toString().trim();
    }

    /**
     * 获取所有模式的帮助信息（用于 --help 参数）
     * 
     * @return 帮助文本
     */
    public static String getHelpText() {
        StringBuilder sb = new StringBuilder();
        sb.append("Paimon 写入性能参数调优测试\n\n");
        sb.append("用法: ./run-perf-test.sh [模式]\n\n");
        sb.append("可用模式:\n");
        
        for (TestModeEntry mode : ALL_MODES) {
            sb.append(String.format("  %-14s %s (%s)\n", 
                mode.getPrimaryAlias(), mode.getDescription(), mode.getTestCaseRange()));
        }
        
        sb.append(String.format("\n默认模式: %s (%s)\n", DEFAULT_MODE_ALIAS, DEFAULT_MODE_ID));
        sb.append("自动模式: auto（无需交互，运行全量测试）\n");
        
        return sb.toString();
    }
}
