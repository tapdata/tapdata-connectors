#!/bin/bash
################################################################################
# Paimon 写入性能参数调优测试 - 一键启动脚本
# 用法: ./run-perf-test.sh [mode]
#
# mode 可选值:
#   1  basic        - 基础用例组(TC-01~03) 默认
#   2  all          - 全量测试(所有组)
#   3  nosmallfile  - 无小文件测试(TC-50~53)
#   4  single       - 单个基准用例(TC-01)
#   auto            - 全自动(无需按回车)，运行全量
#   bucket          - 分桶策略(TC-30~35，重点验证bucket=-2)
#   compaction      - 合并策略(TC-40~45)
#   buffer          - 写入缓冲区(TC-10~16)
#   target          - 目标文件大小(TC-20~23)
#   format          - 文件格式&压缩(TC-60~64)
#   pkupdate        - 主键更新(TC-70~73)
#   parallelism     - 写入并行度(TC-80~83)
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

# ── 环境检查 ──────────────────────────────────────────────────────────────────
if ! command -v java &>/dev/null; then
    echo -e "${RED}[ERROR] 未找到 Java，请先安装 JDK 8+${NC}"; exit 1
fi
echo -e "${GREEN}[INFO] Java : $(java -version 2>&1 | head -1)${NC}"

if ! command -v mvn &>/dev/null; then
    echo -e "${RED}[ERROR] 未找到 Maven${NC}"; exit 1
fi
echo -e "${GREEN}[INFO] Maven: $(mvn -version 2>&1 | head -1)${NC}"
echo ""

# ── 获取测试模式 ────────────────────────────────────────────────────────────
TEST_MODE="${1:-}"

# ── 编译主工程 ──────────────────────────────────────────────────────────────
echo -e "${YELLOW}[1/3] 编译 paimon-connector 主工程...${NC}"
mvn clean install -DskipTests -q -f pom.xml
echo -e "${GREEN}      主工程编译完成${NC}"
echo ""

# ── 编译测试代码 ────────────────────────────────────────────────────────────
echo -e "${YELLOW}[2/3] 编译性能测试代码...${NC}"
mvn test-compile -q -f pom-perf-test.xml
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
        -f pom-perf-test.xml \
        -Dexec.mainClass="io.tapdata.connector.paimon.perf.PerformanceTestRunner" \
        -Dexec.cleanupDaemonThreads=false \
        -Dexec.args="${TEST_MODE}" \
        -Dexec.jvmArgs="${JVM_OPTS}"
else
    # 交互式菜单
    echo -e "${BLUE}请选择测试模式 (直接回车默认 1):${NC}"
    echo "  1  basic        - 基础用例组(TC-01~03)"
    echo "  2  all          - 全量测试(所有组)"
    echo "  3  nosmallfile  - 无小文件测试(TC-50~53)"
    echo "  4  single       - 单个基准用例"
    echo "  auto            - 全自动运行(无需按回车)"
    echo "  bucket          - 分桶策略测试(验证bucket=-2)"
    echo "  compaction      - 合并策略测试"
    echo "  buffer          - 缓冲区参数测试"
    echo "  target          - 目标文件大小测试"
    echo "  format          - 文件格式&压缩测试"
    echo ""
    read -r -p "  请输入选项 [1]: " CHOICE
    CHOICE="${CHOICE:-1}"
    echo ""
    echo -e "${BLUE}>> 测试模式: ${CHOICE}${NC}"
    mvn exec:java \
        -f pom-perf-test.xml \
        -Dexec.mainClass="io.tapdata.connector.paimon.perf.PerformanceTestRunner" \
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
