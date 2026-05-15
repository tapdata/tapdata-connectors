#!/bin/bash
################################################################################
# Paimon 写入性能参数调优测试 - 一键启动脚本（独立工程版）
#
# 测试模式配置已统一到 TestModeConfig.java
# 新增测试模式只需修改 TestModeConfig.java，无需修改此脚本
#
# 用法: ./run-perf-test.sh [模式]
#
# 可用模式（详见 TestModeConfig.java）:
#   1  basic        - 基础用例组(TC-01~03) 默认
#   2  all          - 全量测试(所有组)
#   3  nosmallfile  - 无小文件测试(TC-50~54)
#   4  single       - 单个基准用例(TC-01)
#   5  bucket       - 分桶策略测试(TC-30~35)
#   6  compaction   - 合并策略测试(TC-40~45)
#   7  buffer       - 写入缓冲区测试(TC-10~16)
#   8  target       - 目标文件大小测试(TC-20~23)
#   9  format       - 文件格式压缩测试(TC-60~64)
#   10 pkupdate     - 主键更新测试(TC-70~73)
#   11 parallelism  - 写入并行度测试(TC-80~83)
#   auto            - 全自动运行(无需交互)
#   空/无参数       - 交互式选择
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}"
echo "════════════════════════════════════════════════════════════"
echo "  Paimon 1.3.1  写入性能参数调优测试"
echo "  测试仓库: /tmp/paimon-perf-test"
echo "════════════════════════════════════════════════════════════"
echo -e "${NC}"

# ── 帮助信息 ──────────────────────────────────────────────────────────────
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "用法: ./run-perf-test.sh [模式]"
    echo ""
    echo "可用模式:"
    echo "  1  basic        - 基础用例组(TC-01~03)"
    echo "  2  all          - 全量测试(所有组)"
    echo "  3  nosmallfile  - 无小文件测试(TC-50~54)"
    echo "  4  single       - 单个基准用例(TC-01)"
    echo "  5  bucket       - 分桶策略测试(TC-30~35)"
    echo "  6  compaction   - 合并策略测试(TC-40~45)"
    echo "  7  buffer       - 写入缓冲区测试(TC-10~16)"
    echo "  8  target       - 目标文件大小测试(TC-20~23)"
    echo "  9  format       - 文件格式压缩测试(TC-60~64)"
    echo "  10 pkupdate     - 主键更新测试(TC-70~73)"
    echo "  11 parallelism  - 写入并行度测试(TC-80~83)"
    echo "  auto            - 全自动运行(无需按回车)"
    echo ""
    echo "默认模式: basic (1)"
    exit 0
fi

# ── 环境检查 ──────────────────────────────────────────────────────────────────
if ! command -v java &>/dev/null; then
    echo -e "${RED}[ERROR] 未找到 Java，请先安装 JDK 11+${NC}"; exit 1
fi
echo -e "${GREEN}[INFO] Java : $(java -version 2>&1 | head -1)${NC}"

if ! command -v mvn &>/dev/null; then
    echo -e "${RED}[ERROR] 未找到 Maven${NC}"; exit 1
fi
echo -e "${GREEN}[INFO] Maven: $(mvn -version 2>&1 | head -1)${NC}"
echo ""

# ── 获取测试模式 ────────────────────────────────────────────────────────────
TEST_MODE="${1:-}"

# ── 编译 paimon-connector 主工程（兄弟目录） ──────────────────────────────
echo -e "${YELLOW}[1/3] 编译 paimon-connector 主工程...${NC}"
mvn clean install -DskipTests -q -f ../paimon-connector/pom.xml
echo -e "${GREEN}      主工程编译完成${NC}"
echo ""

# ── 编译性能测试代码（src/main/java，标准 compile goal） ────────────────────
echo -e "${YELLOW}[2/3] 编译性能测试代码...${NC}"
mvn compile -q
echo -e "${GREEN}      测试代码编译完成${NC}"
echo ""

# ── 运行测试 ─────────────────────────────────────────────────────────────────
echo -e "${YELLOW}[3/3] 启动测试程序...${NC}"
echo ""

# JVM 参数（-Xmx4g 保证有足够堆测试大缓冲区场景）
JVM_OPTS="-Xmx4g -Xms512m -XX:+UseG1GC"

if [ -n "$TEST_MODE" ]; then
    echo -e "${BLUE}>> 测试模式: ${TEST_MODE}${NC}"
    mvn exec:java \
        -Dexec.cleanupDaemonThreads=false \
        -Dexec.args="${TEST_MODE}" \
        -Dexec.jvmArgs="${JVM_OPTS}"
else
    # 交互式菜单
    echo -e "${BLUE}请选择测试模式 (直接回车默认 1):${NC}"
    echo "  1  basic        - 基础用例组(TC-01~03)"
    echo "  2  all          - 全量测试(所有组)"
    echo "  3  nosmallfile  - 无小文件测试(TC-50~54)"
    echo "  4  single       - 单个基准用例(TC-01)"
    echo "  5  bucket       - 分桶策略测试(TC-30~35)"
    echo "  6  compaction   - 合并策略测试(TC-40~45)"
    echo "  7  buffer       - 写入缓冲区测试(TC-10~16)"
    echo "  8  target       - 目标文件大小测试(TC-20~23)"
    echo "  9  format       - 文件格式压缩测试(TC-60~64)"
    echo "  10 pkupdate     - 主键更新测试(TC-70~73)"
    echo "  11 parallelism  - 写入并行度测试(TC-80~83)"
    echo "  auto            - 全自动运行(无需按回车)"
    echo ""
    read -r -p "  请输入选项 [1]: " CHOICE
    CHOICE="${CHOICE:-1}"
    echo ""
    echo -e "${BLUE}>> 测试模式: ${CHOICE}${NC}"
    mvn exec:java \
        -Dexec.cleanupDaemonThreads=false \
        -Dexec.args="${CHOICE}" \
        -Dexec.jvmArgs="${JVM_OPTS}"
fi

REPORT_FILE="/tmp/paimon-perf-test/test-report.md"
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  测试完成！${NC}"
if [ -f "${REPORT_FILE}" ]; then
    echo -e "${GREEN}  测试报告: ${REPORT_FILE}${NC}"
    echo -e "${YELLOW}  查看报告: cat ${REPORT_FILE}${NC}"
fi
echo -e "${YELLOW}  查看文件: ls -lhR /tmp/paimon-perf-test/TC-*/${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
