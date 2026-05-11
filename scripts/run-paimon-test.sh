#!/bin/bash
################################################################################
# Paimon 性能测试 - 简化执行脚本
# 功能：执行单个测试用例并生成简单报告
# 使用：./scripts/run-paimon-test.sh [test_class] [test_method]
# 示例：./scripts/run-paimon-test.sh TransactionPerformanceTest testDailyPartitionWrite
################################################################################

set -e  # 遇到错误立即退出

# ==================== 配置区域 ====================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CONNECTOR_DIR="${PROJECT_ROOT}/connectors/paimon-connector"
TEST_OUTPUT_DIR="${PROJECT_ROOT}/test-output"
WAREHOUSE_DIR="${TEST_OUTPUT_DIR}/paimon-warehouse"
REPORT_DIR="${TEST_OUTPUT_DIR}/reports"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="${REPORT_DIR}/test-result-${TIMESTAMP}.md"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ==================== 工具函数 ====================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_step() {
    echo -e "\n${BLUE}>>>${NC} $1"
}

# 清理环境
cleanup_environment() {
    log_step "清理测试环境..."
    
    # 清理仓库目录
    if [ -d "$WAREHOUSE_DIR" ]; then
        rm -rf "$WAREHOUSE_DIR"/*
        log_info "已清理仓库目录：$WAREHOUSE_DIR"
    fi
    
    # 创建必要目录
    mkdir -p "$WAREHOUSE_DIR"
    mkdir -p "$REPORT_DIR"
    
    log_success "环境清理完成"
}

# 检查依赖
check_dependencies() {
    log_step "检查依赖环境..."
    
    # 检查 Java
    if ! command -v java &> /dev/null; then
        log_error "未找到 Java，请先安装 JDK 11+"
        exit 1
    fi
    java_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$java_version" -lt 11 ]; then
        log_error "Java 版本过低 ($java_version)，需要 JDK 11+"
        exit 1
    fi
    log_info "Java 版本：$(java -version 2>&1 | head -1)"
    
    # 检查 Maven
    if ! command -v mvn &> /dev/null; then
        log_error "未找到 Maven，请先安装"
        exit 1
    fi
    log_info "Maven 版本：$(mvn -version 2>&1 | head -1)"
    
    # 检查磁盘空间
    available_space=$(df -h "$PROJECT_ROOT" | tail -1 | awk '{print $4}')
    log_info "可用磁盘空间：$available_space"
    
    log_success "依赖检查通过"
}

# 构建项目
build_project() {
    log_step "构建项目..."
    
    cd "$PROJECT_ROOT"
    
    # 先清理
    mvn clean -q
    
    # 编译 paimon-connector 模块
    cd "$CONNECTOR_DIR"
    mvn compile test-compile -q -DskipTests
    
    if [ $? -eq 0 ]; then
        log_success "项目构建成功"
    else
        log_error "项目构建失败"
        exit 1
    fi
}

# 执行测试
run_test() {
    local test_class=$1
    local test_method=$2
    
    log_step "执行测试：${test_class}#${test_method}"
    
    cd "$CONNECTOR_DIR"
    
    # 设置 JVM 参数
    export MAVEN_OPTS="-Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
    
    # 执行测试
    if [ -n "$test_method" ]; then
        mvn test -Dtest="${test_class}#${test_method}" -Dmaven.test.failure.ignore=true
    else
        mvn test -Dtest="${test_class}" -Dmaven.test.failure.ignore=true
    fi
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        log_success "测试执行成功"
    else
        log_warning "测试执行完成（可能有断言失败）"
    fi
    
    return $exit_code
}

# 生成简单报告
generate_simple_report() {
    local test_class=$1
    local test_method=$2
    local exit_code=$3
    
    log_step "生成测试报告..."
    
    cat > "$REPORT_FILE" << EOF
# Paimon 性能测试结果报告

**测试时间**: $(date '+%Y-%m-%d %H:%M:%S')
**测试类**: ${test_class}
**测试方法**: ${test_method:-All}
**执行状态**: $([ $exit_code -eq 0 ] && echo "✅ 成功" || echo "⚠️ 完成（查看日志详情）")

---

## 测试配置

| 配置项 | 值 |
|--------|-----|
| Paimon 版本 | 1.2.0 |
| 存储类型 | 本地文件系统 |
| 仓库路径 | ${WAREHOUSE_DIR} |
| 写入线程数 | 8 |
| 批量提交大小 | 10000 条 |

---

## 测试说明

### 测试数据模型
- **表名**: TransactionDetails
- **字段数**: 29 个字段 + 1 个分区字段
- **单条大小**: 约 1KB
- **主键**: Id (VARCHAR 900)
- **分区**: pt_created_date (INT, 格式 YYYYMMDD)

### 测试场景
1. **按天分区测试** (testDailyPartitionWrite): 分区字段格式 yyyyMMdd
2. **按月分区测试** (testMonthlyPartitionWrite): 分区字段格式 yyyyMM
3. **按年分区测试** (testYearlyPartitionWrite): 分区字段格式 yyyy

---

## 执行日志

测试日志已输出到控制台，详细日志位置：
\`\`\`
${CONNECTOR_DIR}/target/surefire-reports/
\`\`\`

---

## 性能指标参考

基于测试配置，预期性能指标：

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 吞吐量 | ≥5000 条/秒 | 8 线程并发 |
| 小文件占比 | <10% | <32MB 文件 |
| 内存峰值 | <4GB | JVM 堆内存限制 |
| 写放大率 | <3 | Compaction 效率 |

---

## 下一步

1. 查看测试日志了解详细性能数据
2. 对比不同分区策略的性能差异
3. 根据测试结果调整参数配置

---

**报告生成时间**: $(date '+%Y-%m-%d %H:%M:%S')
**测试执行人**: $(whoami)
EOF

    log_success "报告已生成：$REPORT_FILE"
    
    # 显示报告摘要
    echo ""
    echo "========================================"
    echo "  测试报告摘要"
    echo "========================================"
    head -20 "$REPORT_FILE"
    echo "========================================"
    echo "完整报告：$REPORT_FILE"
    echo "========================================"
}

# 显示使用说明
show_usage() {
    echo "========================================"
    echo "  Paimon 性能测试脚本"
    echo "========================================"
    echo ""
    echo "使用方法："
    echo "  $0 [测试类] [测试方法]"
    echo ""
    echo "示例："
    echo "  # 执行按天分区测试"
    echo "  $0 TransactionPerformanceTest testDailyPartitionWrite"
    echo ""
    echo "  # 执行所有分区测试"
    echo "  $0 TransactionPerformanceTest"
    echo ""
    echo "  # 清理环境"
    echo "  $0 clean"
    echo ""
    echo "========================================"
}

# ==================== 主程序 ====================

main() {
    local action="${1:-help}"
    local test_class="${2:-}"
    local test_method="${3:-}"
    
    case "$action" in
        clean)
            cleanup_environment
            exit 0
            ;;
        help|--help|-h)
            show_usage
            exit 0
            ;;
        *)
            test_class="$action"
            test_method="$test_class"
            ;;
    esac
    
    echo "========================================"
    echo "  Paimon 性能测试"
    echo "  版本：1.0 (Paimon 1.2.0)"
    echo "========================================"
    
    # 1. 检查依赖
    check_dependencies
    
    # 2. 清理环境
    cleanup_environment
    
    # 3. 构建项目
    build_project
    
    # 4. 执行测试
    run_test "$test_class" "$test_method"
    local exit_code=$?
    
    # 5. 生成报告
    generate_simple_report "$test_class" "$test_method" "$exit_code"
    
    echo ""
    echo "========================================"
    echo "  测试执行完成！"
    echo "========================================"
    
    exit $exit_code
}

# 执行主程序
main "$@"
